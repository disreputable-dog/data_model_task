import pytest
from sqlalchemy import create_engine

from data_quality_checks import (
    unique_check,
    no_nulls_check,
    type_check,
    data_quality_check,
)

from init_db_connection import (
    init_engine_and_load_data,
    read_excel_to_dataframe,
)


@pytest.fixture
def set_up():
    with init_engine_and_load_data(
        create_engine("sqlite:///:memory:"),
        "orders",
        read_excel_to_dataframe("tests/test_input_data.xlsx"),
    ) as conn:
        yield conn


def test_unique_check(set_up):
    assert unique_check(set_up, "OrderNumber")
    assert unique_check(set_up, "ClientName") == False


def test_no_nulls_check(set_up):
    assert no_nulls_check(set_up, "OrderNumber")
    assert no_nulls_check(set_up, "DeliveryContactNumber") == False


def test_type_check(set_up):
    assert type_check(set_up, "ClientName", "TEXT")
    assert type_check(set_up, "ClientName", "INTEGER") == False


def test_data_quality_check_passes_on_good_report():
    good_report = {
        "all_values_unique": {"OrderNumber": True},
        "all_values_nonnull": {"ProductName": True, "ProductType": True},
        "column_is_correct_type": {"ClientName": True},
        "column_is_multiplied_correctly": {"TotalPrice": True},
    }

    data_quality_check(good_report)


def test_data_quality_check_fails_on_bad_report():
    bad_report = {
        "all_values_unique": {"OrderNumber": False},
        "all_values_nonnull": {"ProductName": True, "ProductType": True},
        "column_is_correct_type": {"ClientName": True},
        "column_is_multiplied_correctly": {"TotalPrice": True},
    }

    with pytest.raises(ValueError, match="Data quality check failed!"):
        data_quality_check(bad_report)
