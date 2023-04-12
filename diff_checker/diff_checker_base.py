import logging
import os
import time
from functools import wraps

import pandas as pd
import psycopg2
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def setup_logger(name, logfile='LOGFILENAME.log'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even DEBUG messages
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
    fh.setFormatter(fh_formatter)

    # create console handler with a INFO log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(message)s', '%Y-%m-%d %H:%M:%S')
    ch.setFormatter(ch_formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def add_exec_time(func):
    @wraps(func)
    def wrapper(*args, **kargs):
        start = time.time()
        result = func(*args, **kargs)
        elapsed_seconds = time.time() - start
        return result, elapsed_seconds

    return wrapper


@add_exec_time
def exec_query_redshift(redshift_conn, sql: str) -> pd.DataFrame:
    df: pd.DataFrame = redshift_conn.exec_query(sql)
    sort_columns: list = df.columns.tolist()
    return df.sort_values(sort_columns).reset_index(drop=True)


@add_exec_time
def exec_query_snowflake(snowflake_conn, sql: str) -> pd.DataFrame:
    df: pd.DataFrame = snowflake_conn.exec_query(sql)
    sort_columns: list = df.columns.tolist()
    return df.sort_values(sort_columns).reset_index(drop=True)


class RedshiftConnector:
    def __init__(self):
        self.redshift_config = {
            'host': os.environ['REDSHIFT_HOST'],
            'user': os.environ['REDSHIFT_USER'],
            'password': os.environ['REDSHIFT_PASSWORD'],
            'port': os.environ['REDSHIFT_PORT'],
            'database': os.environ['REDSHIFT_DATABASE']
        }


    def exec_query(self, query) -> pd.DataFrame:
        with psycopg2.connect(**self.redshift_config) as conn:
            return pd.read_sql(query, conn)


class SnowflakeConnector:
    def __init__(self):
        self.snowflake_config = {
            'user': os.environ['SNOWFLAKE_USER'],
            'password': os.environ['SNOWFLAKE_PASSWORD'],
            'account': os.environ['SNOWFLAKE_ACCOUNT'],
            'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
            'database': os.environ['SNOWFLAKE_DATABASE'],
            'role': os.environ['SNOWFLAKE_ROLE']
        }


    def exec_query(self, query) -> pd.DataFrame:
        # SQLAlchemy sets columns lower case
        with create_engine(URL(**self.snowflake_config)).connect() as conn:
            return pd.read_sql(query, conn)
