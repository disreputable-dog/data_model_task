from sqlalchemy import MetaData, Table, Column, String, Float, Integer, Date, ForeignKey


def ddl(conn):
    metadata = MetaData()

    Table(
        "fact_orders",
        metadata,
        Column("OrderNumber", String, unique=True, nullable=False, primary_key=True),
        Column(
            "DeliveryId",
            Integer,
            ForeignKey("dim_delivery_details.DeliveryId"),
            nullable=False,
        ),
        Column(
            "ProductId",
            Integer,
            ForeignKey("dim_product_details.ProductId"),
            nullable=False,
        ),
        Column(
            "PaymentId",
            Integer,
            ForeignKey("dim_payment_details.PaymentId"),
            nullable=False,
        ),
        Column("TotalPrice", Float, nullable=False),
        Column("Currency", String, nullable=False),
        Column("ProductQuantity", Integer, nullable=False),
        Column("ClientName", String),
    )

    Table(
        "dim_delivery_details",
        metadata,
        Column(
            "DeliveryId", Integer, primary_key=True
        ),
        Column("DeliveryAddress", String, unique=True, nullable=False),
        Column("DeliveryPostcode", String, unique=True, nullable=False),
        Column("ClientName", String),
        Column("DeliveryCity", String),
        Column("DeliveryCountry", String),
        Column("DeliveryContactNumber", String),
    )

    Table(
        "dim_product_details",
        metadata,
        Column(
            "ProductId", Integer, primary_key=True
        ),
        Column("ProductName", String, unique=True, nullable=False),
        Column("ProductType", String),
        Column("UnitPrice", Float, nullable=False),
    )

    Table(
        "dim_payment_details",
        metadata,
        Column(
            "PaymentId", Integer, primary_key=True
        ),
        Column("PaymentBillingCode", String, unique=True, nullable=False),
        Column("PaymentType", String),
        Column("PaymentDate", Date),
    )

    metadata.create_all(conn, checkfirst=True)
