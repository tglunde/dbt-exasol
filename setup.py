#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-exasol"
package_version = "1.0.0"
description = """The exasol adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Torsten Glunde',
    author_email='torsten@glunde.de',
    maintainer='Ilija Kutle',
    url='https://www.glunde.de',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/exasol/dbt_project.yml',
            'include/exasol/macros/*.sql',
            'include/exasol/macros/**/*.sql',
        ]
    },
    install_requires=[
       'dbt-core>={}'.format('0.19.0'),
       'pyexasol>='.format('0.11')
    ]
)
