import logging

from init_db_connection import init_engine_and_load_data, read_excel_to_dataframe
from data_quality_checks import (
    create_engine,
    data_quality_check,
    run_all_data_quality_checks,
)

from orders_ingestion import ddl

logging.basicConfig(level=logging.INFO, format="%(message)s")


if __name__ == "__main__":
    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "orders",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        data_quality_check(run_all_data_quality_checks(conn))

    with init_engine_and_load_data(
        create_engine("sqlite:////app/hello.db"),
        "orders",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        ddl(conn)
