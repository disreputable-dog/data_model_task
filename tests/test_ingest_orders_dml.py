import datetime

from sqlalchemy import create_engine, MetaData, Table, text
import pytest

from init_db_connection import (
    init_engine_and_load_data,
    read_excel_to_dataframe,
)
from ingest_orders_dml import (
    insert_into_dim_delivery_details,
    insert_into_dim_product_details,
    insert_into_dim_payment_details,
    insert_into_fact_orders,
)
from ddl import create_orders_tables


@pytest.fixture
def conn():
    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "staging",
        read_excel_to_dataframe("tests/test_input_data.xlsx"),
    ) as conn:
        yield conn


@pytest.fixture
def set_up_ddl(conn):
    create_orders_tables(conn)


def insert_additional_data_to_stage(conn):
    """
    Inserts additional data, so that we can test how we handle slowly changing dimensions.
    Duplicate data is inserted in the first row,
    in the second row the a clients' name changes,
    in the third row an existing clients' address changes
    """

    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO staging (OrderNumber, ClientName, ProductName, ProductType, UnitPrice, ProductQuantity, TotalPrice, Currency, DeliveryAddress, DeliveryCity, DeliveryPostcode, DeliveryCountry, DeliveryContactNumber, PaymentType, PaymentBillingCode, PaymentDate)
            VALUES 
            ('PO0060590-1', 'MacGyver Inc', 'Piano', 'Keyboard', 5000, 3, 14100, 'GBP', '72 Academy Street', 'Swindon', 'SN4 9QP', 'United Kingdom', '+44 7911 843910', 'Debit', 'PO0060504-20210321', '21/03/2021'),
            ('PO0060591-1', 'MacGyver & Mustard Inc', 'Piano', 'Keyboard', 5000, 4, 20000, 'GBP', '72 Academy Street', 'Swindon', 'SN4 9QP', 'United Kingdom', '+44 7911 843910', 'Debit', 'PO0060504-20210321', '03/08/2022'),
            ('PO0060592-1', 'Quitzon, Luettgen and Waters', 'Guitar', 'Strings', 800, 4, 20000, 'GBP', '84 Delancey Street', 'Camden', 'NW1 7SA', 'United Kingdom', '+44 7525 312910', 'Debit', 'PO0060504-20210321', '17/10/2023');
            """
        )
    )


def test_insert_into_dim_delivery_details(set_up_ddl, conn):
    """
    Tests that inserting into dim delivery works as expected
    """

    insert_into_dim_delivery_details(conn)

    dim_delivery_details = Table("dim_delivery_details", MetaData(), autoload_with=conn)

    results = [
        row._asdict() for row in conn.execute(dim_delivery_details.select()).fetchall()
    ]

    assert len(results) == 3
    assert results[0]["DeliveryAddress"] == "45 Park Avenue"
    assert results[1]["DeliveryPostcode"] == "SN4 9QP"


def test_sld_insert_into_dim_product_details(set_up_ddl, conn):
    """
    Tests that the SQL type 1 slowly changing dimensions is working correctly
    """

    insert_additional_data_to_stage(conn)
    insert_into_dim_product_details(conn)

    dim_product_details = Table("dim_product_details", MetaData(), autoload_with=conn)

    results = [
        row._asdict() for row in conn.execute(dim_product_details.select()).fetchall()
    ]

    assert (
        len(results) == 5
    ), "Unexpect row count. Should remain as 4 as we're just updating"
    assert (
        results[2]["UnitPrice"] == 5000
    ), "The price of existing piano data was not incremented"


def test_idempotency_insert_into_dim_payment_details(set_up_ddl, conn):
    """
    Tests that the insert query is idempotent - we run it twice and
    expect the result not to change
    """

    insert_into_dim_payment_details(conn)

    dim_payment_details = Table("dim_payment_details", MetaData(), autoload_with=conn)

    results = [
        row._asdict() for row in conn.execute(dim_payment_details.select()).fetchall()
    ]

    insert_into_dim_payment_details(conn)

    results_as_dicts_2 = [
        row._asdict() for row in conn.execute(dim_payment_details.select()).fetchall()
    ]

    assert results == results_as_dicts_2, "dim_payment_details is not idempotent"


def test_idempotency_insert_into_dim_delivery_details(set_up_ddl, conn):
    """
    Tests that the insert query is idempotent - we run it twice and
    expect the result not to change
    """

    insert_into_dim_delivery_details(conn)

    dim_delivery_details = Table("dim_delivery_details", MetaData(), autoload_with=conn)

    results = [
        row._asdict() for row in conn.execute(dim_delivery_details.select()).fetchall()
    ]

    insert_into_dim_delivery_details(conn)

    results_as_dicts_2 = [
        row._asdict() for row in conn.execute(dim_delivery_details.select()).fetchall()
    ]

    assert results == results_as_dicts_2, "dim_delivery_details is not idempotent"


def test_sld_insert_into_dim_delivery_details(set_up_ddl, conn):
    """
    Tests that the SQL type 2 slowly changing dimensions is working correctly
    """

    insert_into_dim_delivery_details(conn)
    dim_delivery_details = Table("dim_delivery_details", MetaData(), autoload_with=conn)

    results_as_dicts_original = [
        row._asdict() for row in conn.execute(dim_delivery_details.select()).fetchall()
    ]

    insert_additional_data_to_stage(conn)
    insert_into_dim_delivery_details(conn)

    results_as_dicts_new = [
        row._asdict() for row in conn.execute(dim_delivery_details.select()).fetchall()
    ]

    assert results_as_dicts_original != results_as_dicts_new
    assert results_as_dicts_new[1] == {
        "DeliveryId": 2,
        "ClientName": "MacGyver Inc",
        "DeliveryAddress": "72 Academy Street",
        "DeliveryPostcode": "SN4 9QP",
        "DeliveryCity": "Swindon",
        "DeliveryCountry": "United Kingdom",
        "DeliveryContactNumber": "+44 7911 843910",
        "ValidFrom": datetime.date(2023, 10, 30),
        "ValidTo": datetime.date(2023, 10, 30),
        "MostRecent": 0,
    } and results_as_dicts_new[3] == {
        "DeliveryId": 4,
        "ClientName": "MacGyver & Mustard Inc",
        "DeliveryAddress": "72 Academy Street",
        "DeliveryPostcode": "SN4 9QP",
        "DeliveryCity": "Swindon",
        "DeliveryCountry": "United Kingdom",
        "DeliveryContactNumber": "+44 7911 843910",
        "ValidFrom": datetime.date(2023, 10, 30),
        "ValidTo": None,
        "MostRecent": 1,
    }, "The SCD didn't work properly"


def test_fact_orders_consistency(set_up_ddl, conn):
    """
    Tests that that the output is expected when joining the deliverys dim
    and the orders fact table
    """

    insert_into_dim_delivery_details(conn)
    insert_into_dim_product_details(conn)
    insert_into_dim_payment_details(conn)
    insert_into_fact_orders(conn)

    result = conn.execute(
        text(
            """
            SELECT 
                d.DeliveryId, 
                d.ClientName, 
                d.DeliveryAddress, 
                d.DeliveryPostcode, 
                d.DeliveryCity, 
                d.DeliveryCountry, 
                d.DeliveryContactNumber, 
                d.ValidFrom, 
                d.ValidTo, 
                d.MostRecent,
                SUM(f.TotalPrice) AS TotalPrice
            FROM dim_delivery_details d
            LEFT JOIN fact_orders f ON d.DeliveryId = f.DeliveryId
            GROUP BY 
                d.DeliveryId, 
                d.ClientName, 
                d.DeliveryAddress, 
                d.DeliveryPostcode, 
                d.DeliveryCity, 
                d.DeliveryCountry, 
                d.DeliveryContactNumber, 
                d.ValidFrom, 
                d.ValidTo, 
                d.MostRecent
            ORDER BY d.DeliveryId;
            """
        )
    )

    results = [row._asdict() for row in result]

    assert results[0] == {
        "DeliveryId": 1,
        "ClientName": "Rath - Schroeder",
        "DeliveryAddress": "45 Park Avenue",
        "DeliveryPostcode": "NP80 1OK",
        "DeliveryCity": "NEWPORT",
        "DeliveryCountry": "UK",
        "DeliveryContactNumber": "+44 7457 884830",
        "ValidFrom": "2023-10-30",
        "ValidTo": None,
        "MostRecent": 1,
        "TotalPrice": 57880.0,
    }
