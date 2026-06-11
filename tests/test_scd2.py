"""Unit tests for the Gold SCD Type 2 decision logic in src/transforms.py.

These assert the same tracked-vs-in-place rules the Gold MERGE uses:
city/state changes create a new version; email/full_name/domain changes
update in place; unseen keys are new; identical rows are no-ops.
"""

from src.transforms import plan_scd2_actions

COLS = ["customer_id", "city", "state", "email", "full_name", "domain"]
_BASE = dict(customer_id="c1", city="New York", state="NY",
             email="c1@x.com", full_name="Ann Lee", domain="x.com")


def _current(spark):
    return spark.createDataFrame([tuple(_BASE[c] for c in COLS)], COLS)


def _change(spark, **overrides):
    row = {**_BASE, **overrides}
    return spark.createDataFrame([tuple(row[c] for c in COLS)], COLS)


def _action_for(spark, change_df):
    planned = plan_scd2_actions(_current(spark), change_df)
    return planned.collect()[0]["scd2_action"]


def test_unseen_key_is_new(spark):
    assert _action_for(spark, _change(spark, customer_id="c2")) == "NEW"


def test_tracked_change_expires_and_inserts(spark):
    assert _action_for(spark, _change(spark, city="Los Angeles")) == "EXPIRE_AND_INSERT"
    assert _action_for(spark, _change(spark, state="CA")) == "EXPIRE_AND_INSERT"


def test_inplace_change_updates_in_place(spark):
    assert _action_for(spark, _change(spark, email="new@x.com")) == "UPDATE_IN_PLACE"
    assert _action_for(spark, _change(spark, full_name="Ann B. Lee")) == "UPDATE_IN_PLACE"


def test_identical_row_is_noop(spark):
    assert _action_for(spark, _change(spark)) == "NOOP"
