from typing import Generator
import contextlib

from sqlalchemy import Engine, MetaData, Table
from sqlalchemy.engine.base import Connection
import pandas as pd


def read_excel_to_dataframe(filename: str) -> pd.DataFrame:
    """Reads an Excel file into a Pandas DataFrame."""
    return pd.read_excel(filename, engine="openpyxl")


@contextlib.contextmanager
def init_engine_and_load_data(
    engine: Engine, table_name: str, dataframe: pd.DataFrame
) -> Generator[Connection, None, None]:
    """Initializes the engine and loads the DataFrame into a staging table."""
    
    connection = None
    try:
        dataframe.to_sql(table_name, con=engine, index=False, if_exists="replace")
        connection = engine.connect()
        yield connection
    finally:
        if connection:
            connection.close()
        # Drop the staging table
        Table(table_name, MetaData(), autoload_with=engine).drop(engine)
        engine.dispose()
