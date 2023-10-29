import logging
import os

from sqlalchemy import text


from init_db_connection import init_engine_and_load_data, read_excel_to_dataframe
from data_quality_checks import (
    create_engine,
    data_quality_check,
    run_all_data_quality_checks,
)
from ddl import ddl

logging.basicConfig(level=logging.INFO, format="%(message)s")


if __name__ == "__main__":
    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "orders",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        data_quality_check(run_all_data_quality_checks(conn))

    db_path = "/app/hello.db"
    if not os.path.exists(db_path):
        # Create an empty SQLite database if it doesn't exist
        open(db_path, 'a').close()

    with init_engine_and_load_data(
        create_engine(f"sqlite:///{db_path}"),
        "staging",
        read_excel_to_dataframe("input_data.xlsx"),
    ) as conn:
        ddl(conn)

        conn.execute(
            text(
                """
        INSERT INTO dim_delivery_details (DeliveryAddress, DeliveryPostcode, DeliveryCity, DeliveryCountry, DeliveryContactNumber, ClientName)
        SELECT DeliveryAddress, DeliveryPostcode, DeliveryCity, DeliveryCountry, DeliveryContactNumber, ClientName
        FROM staging
        GROUP BY LOWER(DeliveryAddress), LOWER(DeliveryPostcode)
        """
            )
        )

        conn.execute(
            text(
                """
        INSERT INTO dim_product_details (ProductName, ProductType, UnitPrice)
        SELECT ProductName, ProductType, MAX(UnitPrice) as UnitPrice
        FROM staging
        GROUP BY LOWER(ProductName);
        """
            )
        )

        conn.execute(
            text(
                """
        INSERT INTO dim_payment_details (PaymentBillingCode, PaymentType, PaymentDate)
        SELECT PaymentBillingCode, PaymentType, PaymentDate
        FROM staging
        GROUP BY LOWER(PaymentBillingCode), LOWER(PaymentType), LOWER(PaymentDate)
        """
            )
        )

        conn.execute(
            text(
                """
                INSERT INTO fact_orders (OrderNumber, DeliveryId, ProductId, PaymentId, TotalPrice, Currency, ProductQuantity, ClientName)
                SELECT
                    s.OrderNumber,
                    d.DeliveryId,
                    p.ProductId,
                    pay.PaymentId,
                    s.TotalPrice,
                    s.Currency,
                    s.ProductQuantity,
                    s.ClientName
                FROM staging s
                JOIN dim_delivery_details d ON LOWER(s.DeliveryAddress) = LOWER(d.DeliveryAddress) AND LOWER(s.DeliveryPostcode) = LOWER(d.DeliveryPostcode)
                JOIN dim_product_details p ON LOWER(s.ProductName) = LOWER(p.ProductName)
                JOIN dim_payment_details pay ON LOWER(s.PaymentBillingCode) = LOWER(pay.PaymentBillingCode);
                """
            )
        )

        conn.commit()
