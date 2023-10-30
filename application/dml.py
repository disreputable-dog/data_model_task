from sqlalchemy import text


def insert_into_dim_delivery_details(conn):
    # Update existing records for the client if they have changed
    conn.execute(
        text(
            """
            UPDATE dim_delivery_details
            SET MostRecent = 0, ValidTo = CURRENT_DATE
            WHERE EXISTS (
                SELECT 1 
                FROM staging s
                WHERE LOWER(s.DeliveryAddress) = LOWER(dim_delivery_details.DeliveryAddress) AND LOWER(s.DeliveryPostcode) = LOWER(dim_delivery_details.DeliveryPostcode) AND LOWER(s.ClientName) = LOWER(dim_delivery_details.ClientName)
                AND dim_delivery_details.MostRecent = 1 
                AND (
                    LOWER(s.DeliveryAddress) != LOWER(dim_delivery_details.DeliveryAddress) OR
                    LOWER(s.DeliveryPostcode) != LOWER(dim_delivery_details.DeliveryPostcode) OR
                    LOWER(s.ClientName) != LOWER(dim_delivery_details.ClientName)
                )
            );
            """
        )
    )

    # Insert new records only if a MostRecent version doesn't exist or has changed
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_delivery_details 
                (ClientName, DeliveryAddress, DeliveryPostcode, DeliveryCity, DeliveryCountry, DeliveryContactNumber, MostRecent, ValidFrom)
            SELECT 
                s.ClientName, s.DeliveryAddress, s.DeliveryPostcode, s.DeliveryCity, s.DeliveryCountry, s.DeliveryContactNumber, 1 AS MostRecent, CURRENT_DATE AS ValidFrom
            FROM staging s
            LEFT JOIN dim_delivery_details d ON LOWER(s.DeliveryAddress) = LOWER(d.DeliveryAddress) AND LOWER(s.DeliveryPostcode) = LOWER(d.DeliveryPostcode) AND LOWER(s.ClientName) = LOWER(d.ClientName) AND d.MostRecent = 1
            WHERE d.DeliveryId IS NULL AND NOT EXISTS (
                SELECT 1 FROM dim_delivery_details dd
                WHERE LOWER(dd.DeliveryAddress) = LOWER(s.DeliveryAddress) 
                AND LOWER(dd.DeliveryPostcode) = LOWER(s.DeliveryPostcode) 
                AND LOWER(dd.ClientName) = LOWER(s.ClientName)
                AND dd.MostRecent = 0
            )
            GROUP BY LOWER(s.DeliveryAddress), LOWER(s.DeliveryPostcode), LOWER(s.ClientName);
            """
        )
    )


def insert_into_dim_product_details(conn):
    # SCD type 1 to handle price changes
    conn.execute(
        text(
            """
            UPDATE dim_product_details
            SET 
                UnitPrice = (SELECT MAX(UnitPrice) FROM staging WHERE 
                             LOWER(dim_product_details.ProductName) = LOWER(staging.ProductName))
            WHERE EXISTS (
                SELECT 1 FROM staging 
                WHERE LOWER(dim_product_details.ProductName) = LOWER(staging.ProductName)
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
                LOWER(dim_product_details.ProductName) = LOWER(staging.ProductName)
            WHERE dim_product_details.ProductId IS NULL
            GROUP BY LOWER(staging.ProductName);
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
            JOIN dim_delivery_details d ON LOWER(s.ClientName) = LOWER(d.ClientName) AND LOWER(s.DeliveryAddress) = LOWER(d.DeliveryAddress) AND LOWER(s.DeliveryPostcode) = LOWER(d.DeliveryPostcode)
            JOIN dim_product_details p ON LOWER(s.ProductName) = LOWER(p.ProductName)
            JOIN dim_payment_details pay ON LOWER(s.PaymentBillingCode) = LOWER(pay.PaymentBillingCode);
            """
        )
    )
