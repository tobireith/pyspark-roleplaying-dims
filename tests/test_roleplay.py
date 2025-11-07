import pytest
from pyspark.sql import SparkSession, Row
from chispa import assert_df_equality

# Import the function we want to test
from roleplay import _get_roleplaying_df


@pytest.fixture(scope="session")
def spark():
    """Provide a SparkSession for testing (compatible with Databricks and local)."""
    return SparkSession.builder.appName("chispa-tests").getOrCreate()


def test_aliasing_with_overrides_and_auto_prefixing(spark):
    """Tests that explicit mappings override automatic prefixing."""
    # ARRANGE
    source_data = [
        Row(date_key=20250101, full_date="2025-01-01", day_of_week_name="Wednesday"),
        Row(date_key=20250102, full_date="2025-01-02", day_of_week_name="Thursday"),
    ]
    source_df = spark.createDataFrame(source_data)

    role_name = "order_date"
    role_cfg = {
        "columns": {
            "date_key": "order_date_key",      # Explicit override
            "full_date": "order_date"           # Explicit override
        }
        # `day_of_week_name` is not in the map and should be auto-prefixed
    }

    # ACT
    # Get actual result from the logic function
    actual_df = _get_roleplaying_df(source_df, role_name, role_cfg)

    # ASSERT
    # Define the expected result
    expected_data = [
        Row(
            order_date_key=20250101,                # Explicitly mapped
            order_date="2025-01-01",                  # Explicitly mapped
            order_date_day_of_week_name="Wednesday" # Automatically prefixed
        ),
        Row(
            order_date_key=20250102,
            order_date="2025-01-02",
            order_date_day_of_week_name="Thursday"
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Assert DataFrame equality
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_filtering_with_automatic_aliasing(spark):
    """Tests if filtering works correctly with the automatic aliasing logic."""
    # ARRANGE
    source_data = [
        Row(date_key=20250103, full_date="2025-01-03", is_weekend=False),
        Row(date_key=20250104, full_date="2025-01-04", is_weekend=True),
    ]
    source_df = spark.createDataFrame(source_data)

    role_name = "ship_date"
    role_cfg = {
        "columns": {
            "date_key": "ship_date_key" # Explicit override for the key
        },
        "filter": "is_weekend = false", # Filter out weekends
    }

    # ACT
    # Get actual result from the logic function
    actual_df = _get_roleplaying_df(source_df, role_name, role_cfg)

    # ASSERT
    # Define the expected result (only one row should remain)
    expected_data = [
        Row(
            ship_date_key=20250103,         # Explicitly mapped
            ship_date_full_date="2025-01-03", # Automatically prefixed
            ship_date_is_weekend=False      # Automatically prefixed
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Assert DataFrame equality
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
    