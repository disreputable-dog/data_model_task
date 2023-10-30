import logging
import os

from init_db_connection import init_engine_and_load_data, read_excel_to_dataframe
from data_quality_checks import (
    create_engine,
    data_quality_check,
    run_all_data_quality_checks,
)
from ddl import ddl
from dml import (
    insert_into_dim_delivery_details,
    insert_into_dim_product_details,
    insert_into_dim_payment_details,
    insert_into_fact_orders,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")


if __name__ == "__main__":
    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "orders",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        data_quality_check(run_all_data_quality_checks(conn))

    db_path = "/app/databases/hello.db"
    if not os.path.exists(db_path):
        open(db_path, "a").close() # Create an empty SQLite database if it doesn't exist

    with init_engine_and_load_data(
        create_engine(f"sqlite:///{db_path}"),
        "staging",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        ddl(conn)

        insert_into_dim_delivery_details(conn)
        insert_into_dim_product_details(conn)
        insert_into_dim_payment_details(conn)

        insert_into_fact_orders(conn)

        conn.commit()
