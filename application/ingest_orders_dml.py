from sqlalchemy import text


def insert_into_dim_delivery_details(conn):
    """
    Insert DML for the delivery dim table. SQL handles slowly changing dimensions by
    maintain an old record if the clients businessname or postcode/address changes
    """

    conn.execute(
        text(
            """
            UPDATE dim_delivery_details
            SET MostRecent = 0, ValidTo = CURRENT_DATE
            WHERE MostRecent = 1
            AND EXISTS (
                SELECT 1 
                FROM staging s
                WHERE 
                    (TRIM(LOWER(s.DeliveryAddress)) = TRIM(LOWER(dim_delivery_details.DeliveryAddress)) AND
                    TRIM(LOWER(s.DeliveryPostcode)) = TRIM(LOWER(dim_delivery_details.DeliveryPostcode)) AND
                    TRIM(LOWER(s.ClientName)) != TRIM(LOWER(dim_delivery_details.ClientName))) 
                    OR
                    (TRIM(LOWER(s.DeliveryAddress)) != TRIM(LOWER(dim_delivery_details.DeliveryAddress)) AND
                    TRIM(LOWER(s.DeliveryPostcode)) != TRIM(LOWER(dim_delivery_details.DeliveryPostcode)) AND
                    TRIM(LOWER(s.ClientName)) = TRIM(LOWER(dim_delivery_details.ClientName)))
            );
            """
        )
    )

    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_delivery_details 
                (ClientName, DeliveryAddress, DeliveryPostcode, DeliveryCity, DeliveryCountry, DeliveryContactNumber, MostRecent, ValidFrom)
            SELECT 
                s.ClientName, s.DeliveryAddress, s.DeliveryPostcode, s.DeliveryCity, s.DeliveryCountry, s.DeliveryContactNumber, 1 AS MostRecent, CURRENT_DATE AS ValidFrom
            FROM staging s
            LEFT JOIN dim_delivery_details d ON TRIM(LOWER(s.DeliveryAddress)) = TRIM(LOWER(d.DeliveryAddress)) AND TRIM(LOWER(s.DeliveryPostcode)) = TRIM(LOWER(d.DeliveryPostcode)) AND TRIM(LOWER(s.ClientName)) = TRIM(LOWER(d.ClientName)) AND d.MostRecent = 1
            WHERE d.DeliveryId IS NULL AND NOT EXISTS (
                SELECT 1 FROM dim_delivery_details dd
                WHERE TRIM(LOWER(dd.DeliveryAddress)) = TRIM(LOWER(s.DeliveryAddress))
                AND TRIM(LOWER(dd.DeliveryPostcode)) = TRIM(LOWER(s.DeliveryPostcode)) 
                AND TRIM(LOWER(dd.ClientName)) = TRIM(LOWER(s.ClientName))
            )
            GROUP BY TRIM(LOWER(s.DeliveryAddress)), TRIM(LOWER(s.DeliveryPostcode)), TRIM(LOWER(s.ClientName));
            """
        )
    )


def insert_into_dim_product_details(conn):
    """
    Insert DML for the product details dim table. SQL handles slowly changing dimensions by
    updating the record (not inserting) if the price changes
    """

    conn.execute(
        text(
            """
            UPDATE dim_product_details
            SET 
                UnitPrice = (SELECT MAX(UnitPrice) FROM staging WHERE 
                             TRIM(LOWER(dim_product_details.ProductName)) = TRIM(LOWER(staging.ProductName)))
            WHERE EXISTS (
                SELECT 1 FROM staging 
                WHERE TRIM(LOWER(dim_product_details.ProductName)) = TRIM(LOWER(staging.ProductName))
            );
            """
        )
    )

    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_product_details (ProductName, ProductType, UnitPrice)
            SELECT staging.ProductName, staging.ProductType, MAX(staging.UnitPrice) as UnitPrice
            FROM staging
            LEFT JOIN dim_product_details ON 
                TRIM(LOWER(dim_product_details.ProductName)) = TRIM(LOWER(staging.ProductName))
            WHERE dim_product_details.ProductId IS NULL
            GROUP BY TRIM(LOWER(staging.ProductName));
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
            GROUP BY TRIM(LOWER(PaymentBillingCode)), TRIM(LOWER(PaymentType)), TRIM(LOWER(PaymentDate));
            """
        )
    )


def insert_into_fact_orders(conn):
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO fact_orders (OrderNumber, DeliveryId, ProductId, PaymentId, TotalPrice, Currency, ProductQuantity)
            SELECT
                s.OrderNumber,
                d.DeliveryId,
                p.ProductId,
                pay.PaymentId,
                s.TotalPrice,
                s.Currency,
                s.ProductQuantity
            FROM staging s
            JOIN dim_delivery_details d ON TRIM(LOWER(s.ClientName)) = TRIM(LOWER(d.ClientName)) AND TRIM(LOWER(s.DeliveryAddress)) = TRIM(LOWER(d.DeliveryAddress)) AND TRIM(LOWER(s.DeliveryPostcode)) = TRIM(LOWER(d.DeliveryPostcode))
            JOIN dim_product_details p ON TRIM(LOWER(s.ProductName)) = TRIM(LOWER(p.ProductName))
            JOIN dim_payment_details pay ON TRIM(LOWER(s.PaymentBillingCode)) = TRIM(LOWER(pay.PaymentBillingCode));
            """
        )
    )
