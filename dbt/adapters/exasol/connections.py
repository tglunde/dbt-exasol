# pylint: disable=wrong-import-order
"""
DBT adapter connection implementation for Exasol.
"""
import decimal
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import agate
import dbt.exceptions
import pyexasol
from dbt.adapters.base import Credentials  # type: ignore
from dbt.adapters.sql import SQLConnectionManager  # type: ignore
from dbt.contracts.connection import AdapterResponse, Connection
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.logger import GLOBAL_LOGGER as logger  # type: ignore
from pyexasol import ExaConnection, ExaStatement

from dbt.adapters.exasol.relation import ProtocolVersionType

ROW_SEPARATOR_DEFAULT = "LF" if os.linesep == "\n" else "CRLF"
TIMESTAMP_FORMAT_DEFAULT = "YYYY-MM-DDTHH:MI:SS"


def connect(**kwargs: bool):
    """
    Global connect method initialising ExasolConnection
    """
    if "autocommit" not in kwargs:
        kwargs["autocommit"] = False
    return ExasolConnection(**kwargs)


class ExasolConnection(ExaConnection):

    row_separator: str = ROW_SEPARATOR_DEFAULT
    timestamp_format: str = TIMESTAMP_FORMAT_DEFAULT

    """
    Override to instantiate ExasolCursor
    """

    def cursor(self):
        """Instance of ExasolCursor"""
        return ExasolCursor(self)


@dataclass
class ExasolAdapterResponse(AdapterResponse):
    """
    Override AdapterResponse
    """

    execution_time: Optional[float] = None


@dataclass
class ExasolCredentials(Credentials):
    """Profile parameters for Exasol in dbt profiles.yml"""

    dsn: str
    user: str
    password: str
    database: str
    schema: str
    # optional statements that can be set in profiles.yml
    # some options might interfere with dbt, so caution is advised
    connection_timeout: int = pyexasol.constant.DEFAULT_CONNECTION_TIMEOUT
    socket_timeout: int = pyexasol.constant.DEFAULT_SOCKET_TIMEOUT
    query_timeout: int = pyexasol.constant.DEFAULT_QUERY_TIMEOUT
    compression: bool = False
    encryption: bool = False
    ## Because of potential interference with dbt, the following statements are not (yet) implemented
    # fetch_dict: bool
    # fetch_size_bytes: int
    # lower_ident: bool
    # quote_ident: bool
    # verbose_error: bool
    # debug: bool
    # udf_output_port: int
    protocol_version: str = "v3"
    retries: int = 1
    row_separator: str = ROW_SEPARATOR_DEFAULT
    timestamp_format: str = TIMESTAMP_FORMAT_DEFAULT

    _ALIASES = {"dbname": "database", "pass": "password"}

    @property
    def type(self):
        return "exasol"

    @property
    def unique_field(self):
        return self.dsn

    def _connection_keys(self):
        return (
            "dsn",
            "user",
            "database",
            "schema",
            "connection_timeout",
            "socket_timeout",
            "query_timeout",
            "compression",
            "encryption",
            "protocol_version",
            "row_separator",
            "timestamp_format",
        )


class ExasolConnectionManager(SQLConnectionManager):
    """Managing Exasol connections"""

    TYPE = "exasol"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as yielded_exception:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(yielded_exception, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(yielded_exception)

    @classmethod
    def get_result_from_cursor(cls, cursor: Any) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            # column_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            for idx, col in enumerate(cursor.description):
                column_names.append(col[0])
                if len(rows) > 0 and isinstance(rows[0][idx], str):
                    if col[1] in ["DECIMAL", "BIGINT"]:
                        for rownum, row in enumerate(rows):
                            tmp = list(row)
                            tmp[idx] = decimal.Decimal(row[idx])
                            rows[rownum] = tmp
            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)  # type: ignore

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection
        credentials = cls.get_credentials(connection.credentials)

        # Support protocol versions
        try:
            format_protocol_version = credentials.protocol_version.lower()
            version = ProtocolVersionType(format_protocol_version)

            if version == ProtocolVersionType.V1:
                protocol_version = pyexasol.PROTOCOL_V1
            elif version == ProtocolVersionType.V2:
                protocol_version = pyexasol.PROTOCOL_V2
            else:
                protocol_version = pyexasol.PROTOCOL_V3
        except:
            raise dbt.exceptions.RuntimeException(
                f"{credentials.protocol_version} is not a valid protocol version."
            )

        def _connect():
            conn = connect(
                dsn=credentials.dsn,
                user=credentials.user,
                password=credentials.password,
                autocommit=True,
                connection_timeout=credentials.connection_timeout,
                socket_timeout=credentials.socket_timeout,
                query_timeout=credentials.query_timeout,
                compression=credentials.compression,
                encryption=credentials.encryption,
                protocol_version=protocol_version,
            )
            # exasol adapter specific attributes that are unknown to pyexasol
            # those can be added to ExasolConnection as members
            conn.row_separator = credentials.row_separator
            conn.timestamp_format = credentials.timestamp_format
            conn.execute(
                f"alter session set NLS_TIMESTAMP_FORMAT='{conn.timestamp_format}'"
            )

            return conn

        retryable_exceptions = [pyexasol.ExaError]

        return cls.retry_connection(
            connection,
            connect=_connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    def commit(self):
        connection = self.get_thread_connection()
        if dbt.flags.STRICT_MODE:  # type: ignore
            assert isinstance(connection, ExaConnection)

        logger.debug(f"On {connection.name}: COMMIT")
        self.add_commit_query()

        connection.transaction_open = False

        return connection

    def begin(self):
        connection = self.get_thread_connection()

        if dbt.flags.STRICT_MODE:
            assert isinstance(connection, ExaConnection)

        if connection.transaction_open is True:
            raise dbt.exceptions.InternalException(
                f"Tried to begin a new transaction on connection {connection.name}, "
                "but it already had one open!"
            )

        connection.transaction_open = True
        return connection

    def cancel(self, connection):
        connection.abort_query()  # type: ignore

    @classmethod
    def get_status(cls, cursor):
        """Override status"""
        return "OK"

    @classmethod
    def get_response(cls, cursor) -> ExasolAdapterResponse:
        return ExasolAdapterResponse(
            _message="OK",
            rows_affected=cursor.rowcount,
            execution_time=cursor.execution_time,
        )

    @classmethod
    def get_credentials(cls, credentials):
        """override get_credentials"""
        return credentials

    def add_query(
        self,
        sql,
        auto_begin=True,
        bindings: Optional[Any] = None,
        abridge_sql_log=False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql
            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))

            pre = time.time()

            if sql.startswith("0CSV|"):
                cursor = connection.handle.cursor().import_from_file(bindings, sql.split("|", 1)[1])  # type: ignore
            else:
                cursor = connection.handle.cursor().execute(sql)  # type: ignore

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre)),
                )
            )
            return connection, cursor


class ExasolCursor(object):
    """Exasol dbt-adapter cursor implementation"""

    array_size = 1

    def __init__(self, connection):
        self.connection = connection
        self.stmt = None

    def import_from_file(self, agate_table, table):
        """importing csv skip=1 parameter for header row"""
        self.connection.import_from_file(
            agate_table.original_abspath,
            (table.split(".")[0], table.split(".")[1]),
            import_params={"skip": 1, "row_separator": self.connection.row_separator},
        )
        return self

    def execute(self, query):
        """executing query"""
        self.stmt = self.connection.execute(query)
        return self

    def executemany(self, query):
        """execute many not implemented yet"""
        raise RuntimeError

    def fetchone(self):
        if self.stmt is None:
            raise RuntimeError("Cannot fetch on unset statement")
        return self.stmt.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            size = self.array_size

        if self.stmt is None:
            raise RuntimeError("Cannot fetch on unset statement")
        return self.stmt.fetchmany(size)

    def fetchall(self):
        if self.stmt is None:
            raise RuntimeError("Cannot fetch on unset statement")
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
        if self.stmt is None:
            return cols

        if "resultSet" != self.stmt.result_type:
            return None

        for k, v in self.stmt.columns().items():
            cols.append(
                (
                    k,
                    v.get("type", None),
                    v.get("size", None),
                    v.get("size", None),
                    v.get("precision", None),
                    v.get("scale", None),
                    True,
                )
            )

        return cols

    @property
    def rowcount(self):
        if self.stmt is not None:
            return self.stmt.rowcount()
        return 0

    @property
    def execution_time(self):
        if self.stmt is not None:
            return self.stmt.execution_time
        return 0

    def close(self):
        if self.stmt is not None:
            self.stmt.close()
