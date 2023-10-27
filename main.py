import logging

import pandas as pd
from pprint import pformat
from sqlalchemy import create_engine, text


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log")],
)
engine = create_engine("sqlite:///:memory:")


def load_excel_data_to_sqlite(engine, excel_data):
    pd.read_excel(excel_data, engine="openpyxl").to_sql(
        "orders", con=engine, index=False, if_exists="replace"
    )


def unique_check(engine, col) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                f"""
            SELECT {col}, COUNT({col}) as count
            FROM orders
            GROUP BY {col}
            HAVING count > 1;
            """
            )
        ).fetchall()

        return len(result) == 0


def no_nulls_check(engine, col) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                f"""
            SELECT NOT EXISTS(
                SELECT 1
                FROM orders
                WHERE {col} IS NULL
            );
            """
            )
        ).scalar()

        return bool(result)


def type_check(engine, col, dtype) -> bool:
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(orders);")).fetchall()

        col_info = next(
            (row for row in result if row[1] == col), None
        )  # 1st index in tuple is the column name
        col_type = col_info[2]  # 2nd index in tuple is the column-tyoe

        return col_type == dtype


def price_calculation_check(engine) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT NOT EXISTS(
                    SELECT 1
                    FROM orders
                    WHERE (UnitPrice * ProductQuantity) != TotalPrice
                );
                """
            )
        ).scalar()

        return bool(result)


def run_all_data_quality_checks(engine) -> dict:
    return {
        "all_values_unique": {"OrderNumber": unique_check(engine, "OrderNumber")},
        "all_values_nonnull": {
            "OrderNumber": no_nulls_check(engine, "OrderNumber"),
            "ProductName": no_nulls_check(engine, "ProductName"),
            "ProductType": no_nulls_check(engine, "ProductType"),
            "UnitPrice": no_nulls_check(engine, "UnitPrice"),
            "ProductQuantity": no_nulls_check(engine, "ProductQuantity"),
            "TotalPrice": no_nulls_check(engine, "TotalPrice"),
            "Currency": no_nulls_check(engine, "Currency"),
            "DeliveryAddress": no_nulls_check(engine, "DeliveryAddress"),
            "DeliveryPostcode": no_nulls_check(engine, "DeliveryPostcode"),
            "PaymentType": no_nulls_check(engine, "PaymentType"),
            "PaymentBillingCode": no_nulls_check(engine, "PaymentBillingCode"),
            "PaymentDate": no_nulls_check(engine, "PaymentDate"),
        },
        "column_is_correct_type": {
            "ClientName": type_check(engine, "ClientName", "TEXT"),
            "OrderNumber": type_check(engine, "OrderNumber", "TEXT"),
            "ProductName": type_check(engine, "ProductName", "TEXT"),
            "ProductType": type_check(engine, "ProductType", "TEXT"),
            "Currency": type_check(engine, "Currency", "TEXT"),
            "DeliveryAddress": type_check(engine, "DeliveryAddress", "TEXT"),
            "DeliveryCity": type_check(engine, "DeliveryCity", "TEXT"),
            "DeliveryPostcode": type_check(engine, "DeliveryPostcode", "TEXT"),
            "DeliveryCountry": type_check(engine, "DeliveryCountry", "TEXT"),
            "PaymentType": type_check(engine, "PaymentType", "TEXT"),
            "PaymentBillingCode": type_check(engine, "PaymentBillingCode", "TEXT"),
            "PaymentDate": type_check(engine, "PaymentDate", "DATETIME"),
        },
        "column_is_multiplied_correctly": {price_calculation_check(engine)},
    }


def data_quality_check(report: dict):
    if any(
        False in v.values() if isinstance(v, dict) else not v for v in report.values()
    ):
        log_message = (
            f"Data quality check has failed. Here's the report for the checks + columns:\n"
            f"{'-' * 60}\n"
            f"{pformat(report, indent=4)}\n"
            f"{'-' * 60}\n\n"
        )

        logging.error(log_message)
        raise ValueError("Data quality check failed!")


load_excel_data_to_sqlite(engine, "input_data.xlsx")
data_quality_check(run_all_data_quality_checks(engine))
