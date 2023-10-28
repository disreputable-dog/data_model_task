from typing import Generator
import contextlib
import logging

from sqlalchemy import Engine
from sqlalchemy.engine.base import Connection
import pandas as pd

from data_quality_checks import create_engine, data_quality_check, run_all_data_quality_checks


logging.basicConfig(level=logging.INFO, format="%(message)s")


def read_excel_to_dataframe(filename: str) -> pd.DataFrame:
    """Reads an Excel file into a Pandas DataFrame."""
    return pd.read_excel(filename, engine="openpyxl")


@contextlib.contextmanager
def init_engine_and_load_data(
    engine: Engine, table_name: str, dataframe: pd.DataFrame
) -> Generator[Connection, None, None]:
    """Initializes the engine and loads the DataFrame into a SQL table."""
    try:
        dataframe.to_sql(table_name, con=engine, index=False, if_exists="replace")
        yield engine.connect()
    finally:
        engine.dispose()

if __name__ == "__main__":
    with init_engine_and_load_data(
            create_engine("sqlite:///:memory:"),
            "orders",
            read_excel_to_dataframe("input_data.xlsx"),
        ) as conn:
            data_quality_check(run_all_data_quality_checks(conn))
