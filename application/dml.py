from sqlalchemy import text


def insert_into_dim_delivery_details(conn):
    conn.execute(
        text(
            """
    INSERT OR IGNORE INTO dim_delivery_details (DeliveryAddress, DeliveryPostcode, DeliveryCity, DeliveryCountry, DeliveryContactNumber, ClientName)
    SELECT DeliveryAddress, DeliveryPostcode, DeliveryCity, DeliveryCountry, DeliveryContactNumber, ClientName
    FROM staging
    GROUP BY LOWER(DeliveryAddress), LOWER(DeliveryPostcode);
    """
        )
    )


def insert_into_dim_product_details(conn):
    conn.execute(
        text(
            """
    INSERT OR IGNORE INTO dim_product_details (ProductName, ProductType, UnitPrice)
    SELECT ProductName, ProductType, MAX(UnitPrice) as UnitPrice
    FROM staging
    GROUP BY LOWER(ProductName);
    """
        )
    )


def insert_into_dim_payment_details(conn):
    conn.execute(
        text(
            """
    INSERT OR IGNORE INTO dim_payment_details (PaymentBillingCode, PaymentType, PaymentDate)
    SELECT PaymentBillingCode, PaymentType, PaymentDate
    FROM staging
    GROUP BY LOWER(PaymentBillingCode), LOWER(PaymentType), LOWER(PaymentDate);
    """
        )
    )


def insert_into_fact_orders(conn):
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO fact_orders (OrderNumber, DeliveryId, ProductId, PaymentId, TotalPrice, Currency, ProductQuantity, ClientName)
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
