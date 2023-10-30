from sqlalchemy import create_engine, MetaData, Table, String, text, cast
import pytest

from init_db_connection import (
    init_engine_and_load_data,
    read_excel_to_dataframe,
)
from dml import (
    insert_into_dim_delivery_details,
    insert_into_dim_product_details,
    insert_into_dim_payment_details,
)
from ddl import ddl


@pytest.fixture
def set_up():
    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "staging",
        read_excel_to_dataframe("tests/test_input_data.xlsx"),
    ) as conn:
        yield conn


@pytest.fixture
def set_up_ddl(set_up):
    ddl(set_up)


@pytest.fixture
def insert_additional_data_to_stage(set_up):
    """
    Inserts additional data, so that we can test how we handle slowly changing dimensions
    """

    set_up.execute(
        text(
            """
    INSERT OR IGNORE INTO staging (OrderNumber, ClientName, ProductName, ProductType, UnitPrice, ProductQuantity, TotalPrice, Currency, DeliveryAddress, DeliveryCity, DeliveryCity, DeliveryCountry, DeliveryContactNumber, PaymentType, PaymentBillingCode, PaymentDate)
    VALUES 
    ('PO0060590-1', 'MacGyver Inc', 'Piano', 'Keyboard', 5000, 3, 14100, 'GBP', '72 Academy Street', 'Swindon', 'SN4 9QP', 'United Kingdom', '+44 7911 843910', 'Debit', 'PO0060504-20210321', '21/03/2021');
    """
        )
    )


def test_insert_into_dim_delivery_details(set_up_ddl, set_up):
    insert_into_dim_delivery_details(set_up)

    metadata = MetaData()
    dim_delivery_details = Table("dim_delivery_details", metadata, autoload_with=set_up)

    results_as_dicts = [
        row._asdict()
        for row in set_up.execute(dim_delivery_details.select()).fetchall()
    ]

    assert len(results_as_dicts) == 3
    assert results_as_dicts[0]["DeliveryAddress"] == "45 Park Avenue"
    assert results_as_dicts[1]["DeliveryPostcode"] == "SN4 9QP"


def test_sld_insert_into_dim_product_details(
    set_up_ddl, insert_additional_data_to_stage, set_up
):
    insert_into_dim_product_details(set_up)

    metadata = MetaData()
    dim_product_details = Table("dim_product_details", metadata, autoload_with=set_up)

    results_as_dicts = [
        row._asdict() for row in set_up.execute(dim_product_details.select()).fetchall()
    ]

    assert (
        len(results_as_dicts) == 4
    ), "Unexpect row count. Should remain as 4 as we're just updating"
    assert (
        results_as_dicts[1]["UnitPrice"] == 5000
    ), "The price of existing piano data was not incremented"


def test_idempotency_insert_into_dim_payment_details(set_up_ddl, set_up):
    """
    Tests that the insert query is idempotent - we run it twice and
    expect the result not to change
    """

    insert_into_dim_payment_details(set_up)

    metadata = MetaData()
    dim_payment_details = Table("dim_payment_details", metadata, autoload_with=set_up)

    results_as_dicts = [
        row._asdict() for row in set_up.execute(dim_payment_details.select()).fetchall()
    ]

    insert_into_dim_payment_details(set_up)

    results_as_dicts_2 = [
        row._asdict() for row in set_up.execute(dim_payment_details.select()).fetchall()
    ]

    assert results_as_dicts == results_as_dicts_2, "dim_payment_details is not idempotent"


def test_idempotency_insert_into_dim_delivery_details(set_up_ddl, set_up):
    """
    Tests that the insert query is idempotent - we run it twice and
    expect the result not to change
    """

    insert_into_dim_delivery_details(set_up)

    metadata = MetaData()
    dim_delivery_details = Table("dim_delivery_details", metadata, autoload_with=set_up)

    results_as_dicts = [
        row._asdict() for row in set_up.execute(dim_delivery_details.select()).fetchall()
    ]

    insert_into_dim_delivery_details(set_up)

    results_as_dicts_2 = [
        row._asdict() for row in set_up.execute(dim_delivery_details.select()).fetchall()
    ]

    assert results_as_dicts == results_as_dicts_2, "dim_delivery_details is not idempotent"


# def test_sld_insert_into_dim_delivery_details(set_up_ddl, insert_additional_data_to_stage, set_up):
#     insert_into_dim_delivery_details(set_up)
#     metadata = MetaData()
#     dim_delivery_details = Table("dim_delivery_details", metadata, autoload_with=set_up)

#     results_as_dicts = [
#         row._asdict() for row in set_up.execute(dim_delivery_details.select()).fetchall()
#     ]

#     print(results_as_dicts)
