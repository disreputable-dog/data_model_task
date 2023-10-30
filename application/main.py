import logging
import os

from init_db_connection import init_engine_and_load_data, read_excel_to_dataframe
from data_quality_checks import (
    create_engine,
    data_quality_check,
    run_all_data_quality_checks,
)
from ddl import create_orders_tables
from ingest_orders_dml import (
    insert_into_dim_delivery_details,
    insert_into_dim_product_details,
    insert_into_dim_payment_details,
    insert_into_fact_orders,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")


def run_data_ingest():
    """
    Runs orders data ingestion in these steps:
        1. Creates a db if doesn't exist (if it isn't past in via a docker volume mount)
        2. Runs data quality checks on in memory db
        3. Creates DDL for on-disk orders db
        4. Inserts data into that db
    """

    db_path = "/app/databases/orders.db"
    if not os.path.exists(db_path):
        open(
            db_path, "a"
        ).close()  # Create an empty SQLite database if it doesn't exist

    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "orders",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        data_quality_check(run_all_data_quality_checks(conn))

    with init_engine_and_load_data(
        create_engine(f"sqlite:///{db_path}"),
        "staging",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        create_orders_tables(conn)

        insert_into_dim_delivery_details(conn)
        insert_into_dim_product_details(conn)
        insert_into_dim_payment_details(conn)

        insert_into_fact_orders(conn)

        conn.commit()


if __name__ == "__main__":
    run_data_ingest()
