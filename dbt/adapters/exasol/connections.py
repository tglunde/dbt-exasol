from dataclasses import dataclass
from contextlib import contextmanager
import time
import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.connection import AdapterResponse
from typing import Optional


import pyexasol
from pyexasol import ExaStatement, ExaConnection


def connect(**kwargs):
    if 'autocommit' not in kwargs:
        kwargs['autocommit'] = False

    return DB2Connection(**kwargs)


class DB2Connection(ExaConnection):
    def cursor(self):
        return ExasolCursor(self)

@dataclass
class ExasolAdapterResponse(AdapterResponse):
    execution_time: Optional[float] = None

@dataclass
class ExasolCredentials(Credentials):
    dsn: str
    user: str
    password: str
    database: str
    schema: str

    _ALIASES = {
        'dbname': 'database',
        'pass': 'password'
    }

    @property
    def type(self):
        return 'exasol'

    def _connection_keys(self):
        return ('dsn', 'user', 'database', 'schema')


class ExasolConnectionManager(SQLConnectionManager):
    TYPE = 'exasol'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as e:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection
        credentials = cls.get_credentials(connection.credentials)
        try:
            C = connect(dsn=credentials.dsn, user=credentials.user,
                        password=credentials.password, autocommit=True)
            connection.handle = C
            connection.state = 'open'

        except Exception as e:
            logger.debug("Got an error when attempting to open an exasol "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def commit(self):
        connection = self.get_thread_connection()
        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, ExaConnection)

        logger.debug('On {}: COMMIT'.format(connection.name))
        self.add_commit_query()

        connection.transaction_open = False

        return connection

    def begin(self):
        connection = self.get_thread_connection()

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, ExaConnection)

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                'Tried to begin a new transaction on connection "{}", but '
                'it already had one open!'.format(connection.get('name')))

        connection.transaction_open = True
        return connection

    def cancel(self, connection):
        connection_name = connection.name
        connection.abort_query()

    @classmethod
    def get_status(cls, cursor):
        return 'OK'

    @classmethod
    def get_response(cls, cursor) -> ExasolAdapterResponse:
        return ExasolAdapterResponse(
            _message='OK',
            rows_affected=cursor.rowcount,
            execution_time=cursor.execution_time,
        )

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def add_query(self, sql, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        logger.debug(sql)
        logger.debug('Using {} connection "{}".'.format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug('On {}: {}....'.format(connection.name, sql[0:512]))
            else:
                logger.debug('On {}: {}'.format(connection.name, sql))
            pre = time.time()

            cursor = connection.handle.cursor().execute(sql)

            logger.debug("SQL status: {} in {:.2f} seconds".format(self.get_status(cursor), (time.time() - pre)))

            return connection, cursor


class ExasolCursor(object):
    arraysize = 1

    def __init__(self, connection):
        self.connection = connection
        self.stmt = None

    def execute(self, query):
        self.stmt = self.connection.execute(query)
        return self

    def executemany(self, query):
        raise RuntimeError

    def fetchone(self):
        return self.stmt.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        return self.stmt.fetchmany(size)

    def fetchall(self):
        return self.stmt.fetchall()

    def nextset(self):
        raise RuntimeError

    def setinputsizes(self):
        pass

    def setoutputsize(self):
        pass

    @property
    def description(self):
        cols = []
        if 'resultSet' != self.stmt.result_type:
            return None

        for k, v in self.stmt.columns().items():
            cols.append((
                k,
                v.get('type', None),
                v.get('size', None),
                v.get('size', None),
                v.get('precision', None),
                v.get('scale', None),
                True
            ))

        return cols

    @property
    def rowcount(self):
        return self.stmt.rowcount()

    @property
    def execution_time(self):
        return self.stmt.execution_time

    def close(self):
        self.stmt.close()
