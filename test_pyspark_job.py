import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pyspark-test") \
        .getOrCreate()
    yield spark
    spark.stop()


def get_dummy_data(spark):
    # 6 dummy records as per instructor: 3 valid + 3 invalid
    data = [
        ("Alice", 100),    # valid -> tax 120.0
        ("Bob", 50),       # valid -> tax 60.0
        ("Charlie", 0),    # invalid: amount <= 0
        ("David", -10),    # invalid: amount <= 0
        (None, 200),       # invalid: NULL name
        ("Eve", 25),       # valid -> tax 30.0
    ]
    return spark.createDataFrame(data, ["name", "amount"])


def test_valid_records_are_kept(spark):
    df = get_dummy_data(spark)
    result = clean_data(df)
    names = sorted([row["name"] for row in result.collect()])
    assert names == ["Alice", "Bob", "Eve"]


def test_amount_le_zero_removed(spark):
    df = get_dummy_data(spark)
    result = clean_data(df)
    amounts = [row["amount"] for row in result.collect()]
    assert 0 not in amounts
    assert -10 not in amounts
    assert all(a > 0 for a in amounts)


def test_null_names_removed(spark):
    df = get_dummy_data(spark)
    result = clean_data(df)
    names = [row["name"] for row in result.collect()]
    assert None not in names


def test_amount_with_tax_calculated(spark):
    df = get_dummy_data(spark)
    result = clean_data(df)
    rows = {row["name"]: row["amount_with_tax"] for row in result.collect()}
    assert rows["Alice"] == pytest.approx(120.0)
    assert rows["Bob"] == pytest.approx(60.0)
    assert rows["Eve"] == pytest.approx(30.0)
