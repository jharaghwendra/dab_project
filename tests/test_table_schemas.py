"""Unit tests for gaming_lakehouse.config.table_schemas.

Idea: table_schemas.py is the single source of truth for all
Silver table configs. These tests make sure the registry is correct
and the lookup function behaves as expected — without needing Spark.

How to read this file (steps):
  Test 1 — are all 5 expected tables registered?
  Test 2 — does every registered table have a non-empty primary_key?
  Test 3 — does every registered table have a non-empty schema?
  Test 4 — does each table's primary_key actually exist as a column in its schema?
  Test 5 — get_table_config() returns the right config for a known table
  Test 6 — get_table_config() returns a safe fallback for an unknown table
  Test 7 — the fallback primary_key follows the naming convention
"""

import pytest
from pyspark.sql.types import StructType

from src.gaming_lakehouse.config.table_schemas import (
    TABLE_CONFIGS,
    TableConfig,
    get_table_config,
)

# ---------------------------------------------------------------------------
# Constants — update this list whenever a new table is added to TABLE_CONFIGS
# ---------------------------------------------------------------------------
EXPECTED_TABLES = ["gametransaction", "userdata", "payment", "check", "gameround"]


# ---------------------------------------------------------------------------
# Test 1 — all expected tables are registered
# ---------------------------------------------------------------------------
def test_01_all_expected_tables_are_registered():
    # Test: TABLE_CONFIGS must contain every table we know about.
    # If someone removes a table by accident this test will catch it.
    for table in EXPECTED_TABLES:
        assert table in TABLE_CONFIGS, f"'{table}' is missing from TABLE_CONFIGS"


# ---------------------------------------------------------------------------
# Test 2 — every registered table has a non-empty primary_key
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table_name", EXPECTED_TABLES)
def test_02_every_table_has_non_empty_primary_key(table_name):
    # Test: the primary_key is used in the MERGE ON condition.
    # An empty string would cause a silent bug — the merge would fail or match nothing.
    config = TABLE_CONFIGS[table_name]
    assert isinstance(config.primary_key, str), f"{table_name}: primary_key must be a string"
    assert config.primary_key.strip() != "", f"{table_name}: primary_key must not be empty"


# ---------------------------------------------------------------------------
# Test 3 — every registered table has a non-empty StructType schema
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table_name", EXPECTED_TABLES)
def test_03_every_table_has_non_empty_schema(table_name):
    # Test: an empty schema would mean no columns are cast — all data lands as null.
    config = TABLE_CONFIGS[table_name]
    assert isinstance(config.schema, StructType), f"{table_name}: schema must be a StructType"
    assert len(config.schema.fields) > 0, f"{table_name}: schema must have at least one field"


# ---------------------------------------------------------------------------
# Test 4 — the primary_key column actually exists in the table's schema
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table_name", EXPECTED_TABLES)
def test_04_primary_key_column_exists_in_schema(table_name):
    # Test: the MERGE condition is `target.{pk} = source.{pk}`.
    # If pk is not a column in the schema, the merge fails with AnalysisException.
    config = TABLE_CONFIGS[table_name]
    column_names = [field.name for field in config.schema.fields]
    assert config.primary_key in column_names, (
        f"{table_name}: primary_key '{config.primary_key}' is not a column in its schema. Found: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 5 — get_table_config() returns the correct config for a known table
# ---------------------------------------------------------------------------
def test_05_get_table_config_returns_correct_config_for_known_table():
    # Test: calling get_table_config("gametransaction") should give back
    # exactly the same object that is stored in TABLE_CONFIGS["gametransaction"].
    config = get_table_config("gametransaction")
    assert config is TABLE_CONFIGS["gametransaction"]
    assert config.primary_key == "gameTransactionId"


# ---------------------------------------------------------------------------
# Test 6 — get_table_config() returns a safe fallback for an unknown table
# ---------------------------------------------------------------------------
def test_06_get_table_config_returns_fallback_for_unknown_table():
    # Test: during rollout of new tables we haven't fully defined yet,
    # the silver script must not crash — it gets a minimal working config instead.
    config = get_table_config("unknowntable")
    assert isinstance(config, TableConfig)
    assert isinstance(config.schema, StructType)
    assert len(config.schema.fields) > 0
    assert config.primary_key != ""


# ---------------------------------------------------------------------------
# Test 7 — the fallback primary_key follows the {table_name}Id convention
# ---------------------------------------------------------------------------
def test_07_fallback_primary_key_follows_naming_convention():
    # Test: fallback pk is "{table_name}Id" so the merge column can be
    # inferred from the table name without any extra config.
    config = get_table_config("wallet")
    assert config.primary_key == "walletId"
