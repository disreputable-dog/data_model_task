from sqlalchemy import create_engine, MetaData, Table
import pytest

from init_db_connection import (
    init_engine_and_load_data,
    read_excel_to_dataframe,
)
from dml import insert_into_dim_delivery_details
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


def test_insert_into_dim_delivery_details(set_up_ddl, set_up):
    insert_into_dim_delivery_details(set_up)

    metadata = MetaData()
    dim_delivery_details = Table("dim_delivery_details", metadata, autoload_with=set_up)

    results = set_up.execute(dim_delivery_details.select()).fetchall()
    results_as_dicts = [row._asdict() for row in results]

    # Let's print out the results to see them in dictionary form
    print(results_as_dicts)

    assert len(results_as_dicts) == 3
    assert results_as_dicts[0]['DeliveryAddress'] == '45 Park Avenue'
    assert results_as_dicts[1]['DeliveryPostcode'] == 'SN4 9QP'


    print("-" * 50)
    for column in dim_delivery_details.c:
        print(column.name, end="\t")
    print("\n" + "-" * 50)
    
    # Print rows
    for row in results:
        for col in row:
            print(col, end="\t")
        print()
