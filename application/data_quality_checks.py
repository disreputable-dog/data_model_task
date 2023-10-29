import logging
from pprint import pformat

from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Connection

logging.basicConfig(level=logging.INFO, format="%(message)s")


def unique_check(conn: Connection, col) -> bool:
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


def no_nulls_check(conn: Connection, col) -> bool:
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


def type_check(conn: Connection, col: str, dtype: str) -> bool:
    result = conn.execute(text("PRAGMA table_info(orders);")).fetchall()

    col_info = next(
        (row for row in result if row[1] == col), None
    )  # 1st index in tuple is the column name
    col_type = col_info[2]  # 2nd index in tuple is the column-tyoe

    return col_type == dtype


def price_calculation_check(conn: Connection) -> bool:
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


def run_all_data_quality_checks(conn: Connection) -> dict:
    return {
        "all_values_unique": {"OrderNumber": unique_check(conn, "OrderNumber")},
        "all_values_nonnull": {
            "OrderNumber": no_nulls_check(conn, "OrderNumber"),
            "ProductName": no_nulls_check(conn, "ProductName"),
            "ProductType": no_nulls_check(conn, "ProductType"),
            "UnitPrice": no_nulls_check(conn, "UnitPrice"),
            "ProductQuantity": no_nulls_check(conn, "ProductQuantity"),
            "TotalPrice": no_nulls_check(conn, "TotalPrice"),
            "Currency": no_nulls_check(conn, "Currency"),
            "DeliveryAddress": no_nulls_check(conn, "DeliveryAddress"),
            "DeliveryPostcode": no_nulls_check(conn, "DeliveryPostcode"),
            "PaymentType": no_nulls_check(conn, "PaymentType"),
            "PaymentBillingCode": no_nulls_check(conn, "PaymentBillingCode"),
            "PaymentDate": no_nulls_check(conn, "PaymentDate"),
        },
        "column_is_correct_type": {
            "ClientName": type_check(conn, "ClientName", "TEXT"),
            "OrderNumber": type_check(conn, "OrderNumber", "TEXT"),
            "ProductName": type_check(conn, "ProductName", "TEXT"),
            "ProductType": type_check(conn, "ProductType", "TEXT"),
            "Currency": type_check(conn, "Currency", "TEXT"),
            "DeliveryAddress": type_check(conn, "DeliveryAddress", "TEXT"),
            "DeliveryCity": type_check(conn, "DeliveryCity", "TEXT"),
            "DeliveryPostcode": type_check(conn, "DeliveryPostcode", "TEXT"),
            "DeliveryCountry": type_check(conn, "DeliveryCountry", "TEXT"),
            "PaymentType": type_check(conn, "PaymentType", "TEXT"),
            "PaymentBillingCode": type_check(conn, "PaymentBillingCode", "TEXT"),
            "PaymentDate": type_check(conn, "PaymentDate", "DATETIME"),
        },
        "column_is_multiplied_correctly": {
            "TotalPrice": f"{price_calculation_check(conn)}"
        },
    }


def generate_log_message(report: dict) -> str:
    return (
        f"Data quality check has failed. Here's the report for the checks + columns:\n"
        f"{'-' * 60}\n"
        f"{pformat(report, indent=4)}\n"
        f"{'-' * 60}\n\n"
    )


def data_quality_check(report: dict):
    if any(
        False in v.values() if isinstance(v, dict) else not v for v in report.values()
    ):
        log_message = generate_log_message(report)
        logging.error(log_message)
        raise ValueError("Data quality check failed!")
