"""
Pure PySpark transformation logic for the Ecommerce Lakehouse pipeline.

The cleaning, flagging, dedup and SCD2-decision logic is factored out of the
Silver/Gold notebooks into plain functions here so that:

  1. the notebooks can import the *same* code that is tested
     (e.g. ``from src.transforms import cleaned_customers, flag_customers``), and
  2. the rules can be unit-tested without a running Databricks workspace
     (see ``tests/``).

Only pure, table-free logic lives here. The Delta ``MERGE`` plumbing stays in
the notebooks; ``plan_scd2_actions`` mirrors the exact tracked-vs-in-place
conditions those merges use, so the SCD2 decision is testable in isolation.
"""

from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# --- shared rules ---------------------------------------------------------
EMAIL_REGEX = r"^[^\s@]+@[^\s@]+\.[^\s@]+$"
MAX_QUANTITY = 1000
MAX_AMOUNT = 1_000_000

CUSTOMER_TRACKED_COLS = ("city", "state")          # change -> new SCD2 version
CUSTOMER_INPLACE_COLS = ("email", "full_name", "domain")  # change -> update in place


# --- Silver: customers ----------------------------------------------------
def cleaned_customers(df: DataFrame) -> DataFrame:
    """Normalise raw customer rows: build full_name, lowercase email, tidy casing."""
    return (
        df.drop("_rescued_data", "source_file")
        .withColumn(
            "full_name",
            F.initcap(F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name")))),
        )
        .drop("first_name", "last_name")
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
    )


def flag_customers(df: DataFrame) -> DataFrame:
    """Attach a ``dq_reason`` string; empty string means the row passed every rule."""
    reason = F.concat_ws(
        "; ",
        F.when(F.col("customer_id").isNull(), F.lit("null customer_id")),
        F.when(F.col("full_name").isNull(), F.lit("null full_name")),
        F.when(F.col("email").isNull(), F.lit("null email")),
        F.when(
            F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_REGEX),
            F.lit("invalid email"),
        ),
    )
    return df.withColumn("dq_reason", reason)


# --- Silver: orders -------------------------------------------------------
def clean_orders(df: DataFrame) -> DataFrame:
    """Cast types, round currency and derive unit_price (null when quantity <= 0)."""
    return (
        df.drop("_rescued_data", "source_file")
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("total_amount", F.round(F.col("total_amount").cast("double"), 2))
        .withColumn(
            "unit_price",
            F.when(
                F.col("quantity") > 0,
                F.round(F.col("total_amount") / F.col("quantity"), 2),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def flag_orders(df: DataFrame) -> DataFrame:
    """Attach a ``dq_reason`` string covering nulls, negatives, ceilings and future dates."""
    reasons = F.concat_ws(
        "; ",
        F.when(F.col("order_id").isNull(), F.lit("order_id")),
        F.when(F.col("customer_id").isNull(), F.lit("customer_id")),
        F.when(F.col("product_id").isNull(), F.lit("product_id")),
        F.when((F.col("quantity").isNull()) | (F.col("quantity") < 0), F.lit("invalid quantity")),
        F.when(F.col("quantity") > MAX_QUANTITY, F.lit("quantity exceeds ceiling")),
        F.when((F.col("total_amount").isNull()) | (F.col("total_amount") < 0), F.lit("invalid total_amount")),
        F.when(F.col("total_amount") > MAX_AMOUNT, F.lit("total_amount exceeds ceiling")),
        F.when(F.col("order_date").isNull(), F.lit("null order_date")),
        F.when(F.col("order_date") > F.current_date(), F.lit("future order_date")),
    )
    return df.withColumn("dq_reason", reasons)


# --- Silver: shared helpers ----------------------------------------------
def split_valid_rejects(flagged: DataFrame):
    """Split a flagged frame into (valid, rejected) on an empty ``dq_reason``."""
    valid = flagged.filter(F.col("dq_reason") == "").drop("dq_reason")
    rejects = flagged.filter(F.col("dq_reason") != "")
    return valid, rejects


def dedup_latest(df: DataFrame, key: str, order_col: str = "ingestion_date") -> DataFrame:
    """Keep one row per ``key`` — the latest by ``order_col``."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# --- Gold: SCD2 decision logic -------------------------------------------
def plan_scd2_actions(
    current_active: DataFrame,
    changes: DataFrame,
    key: str = "customer_id",
    tracked_cols=CUSTOMER_TRACKED_COLS,
    inplace_cols=CUSTOMER_INPLACE_COLS,
) -> DataFrame:
    """Classify each incoming change against the currently-active dimension rows.

    Returns ``changes`` plus an ``scd2_action`` column, mirroring the Gold MERGE:
      * ``NEW``               — key has no active version yet  -> insert
      * ``EXPIRE_AND_INSERT`` — a *tracked* attribute changed  -> close old, insert new version
      * ``UPDATE_IN_PLACE``   — only an in-place attribute changed
      * ``NOOP``              — nothing changed
    """
    cols = list(tracked_cols) + list(inplace_cols)
    cur = current_active.withColumn("_exists", F.lit(True)).select(
        key, "_exists", *[F.col(c).alias(f"cur_{c}") for c in cols]
    )
    joined = changes.join(cur, on=key, how="left")

    tracked_changed = reduce(or_, [~F.col(c).eqNullSafe(F.col(f"cur_{c}")) for c in tracked_cols])
    inplace_changed = reduce(or_, [~F.col(c).eqNullSafe(F.col(f"cur_{c}")) for c in inplace_cols])

    action = (
        F.when(F.col("_exists").isNull(), F.lit("NEW"))
        .when(tracked_changed, F.lit("EXPIRE_AND_INSERT"))
        .when(inplace_changed, F.lit("UPDATE_IN_PLACE"))
        .otherwise(F.lit("NOOP"))
    )

    drop_cols = ["_exists"] + [f"cur_{c}" for c in cols]
    return joined.withColumn("scd2_action", action).drop(*drop_cols)
