import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def create_database_connection() -> Engine:
    server = os.environ['DATABASE_SERVER']
    user = os.environ['DATABASE_USER']
    password = os.environ['DATABASE_PASSWORD']
    port = os.environ['DATABASE_PORT']
    database = os.environ['DATABASE_NAME']

    url = f'mssql+pymssql://{user}:{password}@{server}:{port}/{database}'

    return create_engine(url)


def insert_into_database(df: pd.DataFrame, table: str, if_exists='append', engine_func=create_database_connection):
    LOGGER.info(f'SQL Database - Inserting data into "{table}"')
    engine = engine_func()

    try:
        df.to_sql(table, engine, if_exists=if_exists, index=False)
        LOGGER.info(f'SQL Database - Data inserted into "{table}"')

    except Exception as ex:
        LOGGER.error(f'SQL Database - Failed to insert data into the database {ex}')
        raise ex
    finally:
        engine.dispose()


def get_dataframe_from_database(query: str) -> pd.DataFrame:
    engine = create_database_connection()

    try:
        return pd.read_sql(query, engine)
    except Exception as ex:
        LOGGER.error(f'Failed to get data from the database {ex}')
        raise ex
    finally:
        engine.dispose()


def execute_query(query: str, engine_func=create_database_connection) -> None:
    engine = engine_func()

    try:
        engine.execute(text(query).execution_options(autocommit=True))
    except Exception as ex:
        LOGGER.error(f'Failed to execute query {query}: {ex}')
        raise ex
    finally:
        engine.dispose()
