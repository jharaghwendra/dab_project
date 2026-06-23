"""
gaming_lakehouse.config.table_schemas
======================================
Central registry of Delta/Silver table configurations.

Each entry maps a table_name string to a TableConfig with:
  - primary_key : the unique business key used in MERGE ON condition
  - schema      : PySpark StructType defining the canonical Silver schema contract
"""

from dataclasses import dataclass

from pyspark.sql.types import (
    ByteType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class TableConfig:
    primary_key: str
    schema: StructType
    version_col: str = "version"


_IG_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("player_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("studio_name", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("transaction_reason", StringType(), True),
        StructField("record_version", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("succeeded_at", TimestampType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("amount_eur", DecimalType(34, 4), True),
        StructField("amount_real_eur", DecimalType(34, 4), True),
        StructField("amount_bonus_eur", DecimalType(34, 4), True),
        StructField("provider_amount_eur", DecimalType(34, 4), True),
        StructField("wallet_amount_eur", DecimalType(34, 4), True),
        StructField("tax_amount_eur", DecimalType(34, 4), True),
        StructField("balance_before_eur", DecimalType(34, 4), True),
        StructField("balance_after_eur", DecimalType(34, 4), True),
        StructField("balance_before_real_eur", DecimalType(34, 4), True),
        StructField("balance_after_real_eur", DecimalType(34, 4), True),
        StructField("balance_before_bonus_eur", DecimalType(34, 4), True),
        StructField("balance_after_bonus_eur", DecimalType(34, 4), True),
        StructField("fx_rate", DoubleType(), True),
        StructField("discardable", ByteType(), True),
    ]
)

_IG_PLAYER_SCHEMA = StructType(
    [
        StructField("player_id", StringType(), False),
        StructField("country_code", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("brand_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("modified_at", TimestampType(), True),
        StructField("first_deposit_at", TimestampType(), True),
        StructField("last_deposit_at", TimestampType(), True),
        StructField("last_login_at", TimestampType(), True),
        StructField("first_bet_at", TimestampType(), True),
        StructField("last_bet_at", TimestampType(), True),
        StructField("birth_date", TimestampType(), True),
        StructField("jurisdiction", StringType(), True),
        StructField("risk_tags", StringType(), True),
        StructField("segment_ids", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("verified_kyc", ByteType(), True),
        StructField("is_email_verified", ByteType(), True),
        StructField("is_phone_verified", ByteType(), True),
        StructField("session_limit_remaining", DoubleType(), True),
        StructField("loss_limit_remaining", DecimalType(34, 4), True),
        StructField("deposit_limit_remaining", DecimalType(34, 4), True),
        StructField("bet_limit_remaining", DecimalType(34, 4), True),
        StructField("total_balance", DecimalType(34, 4), True),
        StructField("total_bet", DecimalType(34, 4), True),
        StructField("total_deposit", DecimalType(34, 4), True),
        StructField("total_withdraw", DecimalType(34, 4), True),
    ]
)

_IG_PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("player_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("gateway_name", StringType(), True),
        StructField("status_code", StringType(), True),
        StructField("first_of_type", ByteType(), True),
        StructField("amount_eur", DecimalType(34, 4), True),
        StructField("fee_eur", DecimalType(34, 4), True),
        StructField("converted_amount_eur", DecimalType(34, 4), True),
        StructField("converted_currency_code", StringType(), True),
        StructField("balance_before_eur", DecimalType(34, 4), True),
        StructField("balance_after_eur", DecimalType(34, 4), True),
        StructField("balance_before_real_eur", DecimalType(34, 4), True),
        StructField("balance_after_real_eur", DecimalType(34, 4), True),
        StructField("balance_before_bonus_eur", DecimalType(34, 4), True),
        StructField("balance_after_bonus_eur", DecimalType(34, 4), True),
        StructField("created_at", TimestampType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("succeeded_at", TimestampType(), True),
        StructField("declined_deposit_id", StringType(), True),
        StructField("version", IntegerType(), True),
    ]
)

_IG_COMPLIANCE_CHECK_SCHEMA = StructType(
    [
        StructField("check_id", StringType(), False),
        StructField("player_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("check_type", StringType(), True),
        StructField("check_status", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("modified_at", TimestampType(), True),
        StructField("version", IntegerType(), True),
    ]
)

_IG_ROUND_SCHEMA = StructType(
    [
        StructField("round_id", StringType(), False),
        StructField("player_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("currency_code", StringType(), True),
        StructField("is_promotional", IntegerType(), True),
        StructField("is_closed", IntegerType(), True),
        StructField("bet_amount_eur", DecimalType(34, 4), True),
        StructField("win_amount_eur", DecimalType(34, 4), True),
        StructField("bonus_bet_amount_eur", DecimalType(34, 4), True),
        StructField("bonus_win_amount_eur", DecimalType(34, 4), True),
        StructField("tax_amount_eur", DecimalType(34, 4), True),
        StructField("bet_count", IntegerType(), True),
        StructField("win_count", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("version", IntegerType(), True),
    ]
)

_IG_GAME_CATALOG_SCHEMA = StructType(
    [
        StructField("game_id", StringType(), False),
        StructField("country_code", StringType(), True),
        StructField("game_name", StringType(), True),
        StructField("game_studio", StringType(), True),
        StructField("game_vertical", StringType(), True),
        StructField("game_type", StringType(), True),
        StructField("game_class", StringType(), True),
        StructField("game_slug", StringType(), True),
        StructField("table_id", StringType(), True),
        StructField("is_live", ByteType(), True),
        StructField("is_enabled", ByteType(), True),
        StructField("is_branded", ByteType(), True),
        StructField("is_free_games", ByteType(), True),
        StructField("is_gamble", ByteType(), True),
        StructField("is_progressive_jackpot", ByteType(), True),
        StructField("is_login_required", ByteType(), True),
        StructField("is_bonus_game", ByteType(), True),
        StructField("is_private", ByteType(), True),
        StructField("is_discardable", ByteType(), True),
        StructField("rtp", DecimalType(34, 4), True),
        StructField("volatility", StringType(), True),
        StructField("min_bet", DecimalType(34, 4), True),
        StructField("max_bet", DecimalType(34, 4), True),
        StructField("jackpot_id", StringType(), True),
        StructField("allowed_wallet_type", StringType(), True),
        StructField("released_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

_IG_BRAND_GAME_CATALOG_SCHEMA = StructType(
    [
        StructField("brand_game_id", StringType(), False),
        StructField("game_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("is_enabled", ByteType(), True),
        StructField("private_status", StringType(), True),
        StructField("enabled_at", TimestampType(), True),
        StructField("first_enabled_at", TimestampType(), True),
        StructField("disabled_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

_IG_PLAYER_TAG_SCHEMA = StructType(
    [
        StructField("tag_id", StringType(), False),
        StructField("player_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("tag_category", StringType(), True),
        StructField("is_active", ByteType(), True),
        StructField("is_discardable", ByteType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("modified_at", TimestampType(), True),
        StructField("tag_creator_id", StringType(), True),
        StructField("tag_updater_id", StringType(), True),
        StructField("version", IntegerType(), True),
    ]
)

_IG_PLAYER_LIMIT_SCHEMA = StructType(
    [
        StructField("limit_id", StringType(), False),
        StructField("player_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("limit_type", StringType(), True),
        StructField("limit_status", StringType(), True),
        StructField("limit_value", DecimalType(34, 4), True),
        StructField("limit_value_eur", DecimalType(34, 4), True),
        StructField("limit_value_base", DecimalType(34, 4), True),
        StructField("currency_code", StringType(), True),
        StructField("active_from", TimestampType(), True),
        StructField("active_until", TimestampType(), True),
        StructField("next_reset_time", TimestampType(), True),
        StructField("progress", StringType(), True),
        StructField("creator_id", StringType(), True),
        StructField("modifier_id", StringType(), True),
        StructField("modified_at", TimestampType(), True),
        StructField("version", IntegerType(), True),
    ]
)

TABLE_CONFIGS: dict[str, TableConfig] = {
    "ig_transaction": TableConfig(
        "transaction_id", _IG_TRANSACTION_SCHEMA, "record_version"
    ),
    "ig_player": TableConfig("player_id", _IG_PLAYER_SCHEMA, "modified_at"),
    "ig_payment": TableConfig("payment_id", _IG_PAYMENT_SCHEMA, "version"),
    "ig_compliance_check": TableConfig(
        "check_id", _IG_COMPLIANCE_CHECK_SCHEMA, "modified_at"
    ),
    "ig_round": TableConfig("round_id", _IG_ROUND_SCHEMA, "version"),
    "ig_game_catalog": TableConfig("game_id", _IG_GAME_CATALOG_SCHEMA, "updated_at"),
    "ig_brand_game_catalog": TableConfig(
        "brand_game_id", _IG_BRAND_GAME_CATALOG_SCHEMA, "updated_at"
    ),
    "ig_player_tag": TableConfig("tag_id", _IG_PLAYER_TAG_SCHEMA, "version"),
    "ig_player_limit": TableConfig("limit_id", _IG_PLAYER_LIMIT_SCHEMA, "version"),
}


def get_table_config(table_name: str) -> TableConfig:
    if table_name in TABLE_CONFIGS:
        return TABLE_CONFIGS[table_name]

    return TableConfig(primary_key=f"{table_name}_id", schema=StructType([]))
