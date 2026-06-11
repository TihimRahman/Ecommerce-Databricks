"""Unit tests for the Silver-layer transformation rules in src/transforms.py."""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.transforms import (
    cleaned_customers,
    clean_orders,
    flag_customers,
    flag_orders,
    split_valid_rejects,
    dedup_latest,
)


# --- customers ------------------------------------------------------------
def test_cleaned_customers_normalises_fields(spark):
    df = spark.createDataFrame(
        [("c1", "john", "doe", "  John.Doe@X.COM ", " new york ", "ny")],
        ["customer_id", "first_name", "last_name", "email", "city", "state"],
    )
    out = cleaned_customers(df)
    row = out.collect()[0]
    assert row["full_name"] == "John Doe"
    assert row["email"] == "john.doe@x.com"
    assert row["city"] == "New York"
    assert row["state"] == "NY"
    assert "first_name" not in out.columns and "last_name" not in out.columns


def test_flag_customers_flags_invalid_email_and_passes_clean(spark):
    df = spark.createDataFrame(
        [("c1", "John Doe", "good@example.com"),
         ("c2", "Jane Roe", "not-an-email")],
        ["customer_id", "full_name", "email"],
    )
    reasons = {r["customer_id"]: r["dq_reason"] for r in flag_customers(df).collect()}
    assert reasons["c1"] == ""
    assert "invalid email" in reasons["c2"]


def test_flag_customers_flags_null_id(spark):
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("full_name", StringType()),
        StructField("email", StringType()),
    ])
    df = spark.createDataFrame([(None, "John Doe", "good@example.com")], schema)
    assert "null customer_id" in flag_customers(df).collect()[0]["dq_reason"]


# --- shared helpers -------------------------------------------------------
def test_split_valid_rejects_partitions_on_dq_reason(spark):
    df = spark.createDataFrame(
        [("c1", ""), ("c2", "invalid email")],
        ["customer_id", "dq_reason"],
    )
    valid, rejects = split_valid_rejects(df)
    assert [r["customer_id"] for r in valid.collect()] == ["c1"]
    assert [r["customer_id"] for r in rejects.collect()] == ["c2"]
    assert "dq_reason" not in valid.columns  # dropped from the clean stream


def test_dedup_latest_keeps_newest_per_key(spark):
    df = spark.createDataFrame(
        [("c1", "old", "2024-01-01 00:00:00"),
         ("c1", "new", "2024-06-01 00:00:00"),
         ("c2", "only", "2024-03-01 00:00:00")],
        ["customer_id", "tag", "ingestion_date"],
    ).withColumn("ingestion_date", F.to_timestamp("ingestion_date"))
    out = {r["customer_id"]: r["tag"] for r in dedup_latest(df, "customer_id").collect()}
    assert out == {"c1": "new", "c2": "only"}


# --- orders ---------------------------------------------------------------
def test_clean_orders_derives_unit_price(spark):
    df = spark.createDataFrame(
        [("o1", 4, "20.0", "2024-01-01"),
         ("o2", 0, "10.0", "2024-01-01")],
        ["order_id", "quantity", "total_amount", "order_date"],
    )
    out = {r["order_id"]: r["unit_price"] for r in clean_orders(df).collect()}
    assert out["o1"] == 5.0
    assert out["o2"] is None  # quantity 0 -> null, never a divide-by-zero


def test_flag_orders_catches_negatives_ceilings_and_future_dates(spark):
    df = spark.createDataFrame(
        [("o1", "c1", "p1", 2, 10.0, "2024-01-01"),     # valid
         ("o2", "c1", "p1", -3, 10.0, "2024-01-01"),    # negative quantity
         ("o3", "c1", "p1", 2, 10.0, "2999-01-01"),     # future date
         ("o4", "c1", "p1", 5000, 10.0, "2024-01-01")], # over quantity ceiling
        ["order_id", "customer_id", "product_id", "quantity", "total_amount", "order_date"],
    ).withColumn("order_date", F.to_date("order_date"))
    reasons = {r["order_id"]: r["dq_reason"] for r in flag_orders(df).collect()}
    assert reasons["o1"] == ""
    assert "invalid quantity" in reasons["o2"]
    assert "future order_date" in reasons["o3"]
    assert "quantity exceeds ceiling" in reasons["o4"]
