"""
gaming_lakehouse.config.table_schemas
======================================
Central registry of Delta/Silver table configurations.

Each entry maps a table_name string to a TableConfig with:
  - primary_key : the unique business key used in MERGE ON condition
  - schema      : PySpark StructType mirroring the MySQL target schema

Adding a new table:
  1. Define a StructType for the new table below.
  2. Register it in TABLE_CONFIGS.
  That's it — the silver script picks it up automatically.
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


# ---------------------------------------------------------------------------
# gametransaction
# ---------------------------------------------------------------------------
_GAMETRANSACTION_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("amount", DecimalType(34, 4), True),
        StructField("amountBonus", DecimalType(34, 4), True),
        StructField("amountReal", DecimalType(34, 4), True),
        StructField("amounts", StringType(), True),
        StructField("availableBalanceAfter", DecimalType(34, 4), True),
        StructField("availableBalanceAfterBonus", DecimalType(34, 4), True),
        StructField("availableBalanceAfterReal", DecimalType(34, 4), True),
        StructField("availableBalancesAfter", StringType(), True),
        StructField("balanceAfter", DecimalType(34, 4), True),
        StructField("balanceAfterBonus", DecimalType(34, 4), True),
        StructField("balanceAfterReal", DecimalType(34, 4), True),
        StructField("balancesAfter", StringType(), True),
        StructField("errorCode", StringType(), True),
        StructField("device", StringType(), True),
        StructField("gameId", StringType(), True),
        StructField("message", StringType(), True),
        StructField("promotional", ByteType(), True),
        StructField("providerAmount", DecimalType(34, 4), True),
        StructField("publishable", ByteType(), True),
        StructField("taxAmount", DecimalType(34, 4), True),
        StructField("reason", StringType(), True),
        StructField("sessionId", StringType(), True),
        StructField("status", StringType(), True),
        StructField("studio", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("fxRate", DoubleType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("brandId", StringType(), True),
        StructField("syndicateSessionId", StringType(), True),
        StructField("betslipId", StringType(), True),
        StructField("gameRoundId", StringType(), True),
        StructField("gameTransactionId", StringType(), False),
        StructField("promotionId", StringType(), True),
        StructField("betslip", StringType(), True),
        StructField("betslipStatus", StringType(), True),
        StructField("fxSpread", DoubleType(), True),
        StructField("gameRoundIndex", IntegerType(), True),
        StructField("gameSessionId", StringType(), True),
        StructField("gameTransactionType", StringType(), True),
        StructField("jackpotAmount", DecimalType(34, 4), True),
        StructField("jackpotContribution", DecimalType(34, 4), True),
        StructField("providerBetslipId", StringType(), True),
        StructField("providerGameSessionId", StringType(), True),
        StructField("providerGameTransactionId", StringType(), True),
        StructField("providerRoundId", StringType(), True),
        StructField("transactionReason", StringType(), True),
        StructField("walletAmount", DecimalType(34, 4), True),
        StructField("walletCurrencyCode", StringType(), True),
        StructField("providerSportsbookId", StringType(), True),
        StructField("sportsbook", StringType(), True),
        StructField("sportsbookId", StringType(), True),
        StructField("sportsbookStatus", StringType(), True),
        StructField("betslipSettledAt", TimestampType(), True),
        StructField("shopRef", StringType(), True),
        StructField("terminalRef", StringType(), True),
        StructField("code", StringType(), True),
        StructField("details", StringType(), True),
        StructField("succeededAt", TimestampType(), True),
        StructField("version", IntegerType(), True),
        StructField("discardable", ByteType(), True),
        StructField("amount_eur", DecimalType(34, 4), True),
        StructField("amount_base", DecimalType(34, 4), True),
        StructField("amountBonus_eur", DecimalType(34, 4), True),
        StructField("amountBonus_base", DecimalType(34, 4), True),
        StructField("amountReal_eur", DecimalType(34, 4), True),
        StructField("amountReal_base", DecimalType(34, 4), True),
        StructField("availableBalanceAfter_eur", DecimalType(34, 4), True),
        StructField("availableBalanceAfter_base", DecimalType(34, 4), True),
        StructField("availableBalanceAfterBonus_eur", DecimalType(34, 4), True),
        StructField("availableBalanceAfterBonus_base", DecimalType(34, 4), True),
        StructField("availableBalanceAfterReal_eur", DecimalType(34, 4), True),
        StructField("availableBalanceAfterReal_base", DecimalType(34, 4), True),
        StructField("balanceAfter_eur", DecimalType(34, 4), True),
        StructField("balanceAfter_base", DecimalType(34, 4), True),
        StructField("balanceAfterBonus_eur", DecimalType(34, 4), True),
        StructField("balanceAfterBonus_base", DecimalType(34, 4), True),
        StructField("balanceAfterReal_eur", DecimalType(34, 4), True),
        StructField("balanceAfterReal_base", DecimalType(34, 4), True),
        StructField("betslipWinnings", DecimalType(34, 4), True),
        StructField("betslipWinnings_eur", DecimalType(34, 4), True),
        StructField("betslipWinnings_base", DecimalType(34, 4), True),
        StructField("jackpotAmount_eur", DecimalType(34, 4), True),
        StructField("jackpotAmount_base", DecimalType(34, 4), True),
        StructField("jackpotContribution_eur", DecimalType(34, 4), True),
        StructField("jackpotContribution_base", DecimalType(34, 4), True),
        StructField("providerAmount_eur", DecimalType(34, 4), True),
        StructField("providerAmount_base", DecimalType(34, 4), True),
        StructField("sportsbookWinnings", DecimalType(34, 4), True),
        StructField("sportsbookWinnings_eur", DecimalType(34, 4), True),
        StructField("sportsbookWinnings_base", DecimalType(34, 4), True),
        StructField("taxAmount_eur", DecimalType(34, 4), True),
        StructField("taxAmount_base", DecimalType(34, 4), True),
        StructField("walletAmount_eur", DecimalType(34, 4), True),
        StructField("walletAmount_base", DecimalType(34, 4), True),
    ]
)

# ---------------------------------------------------------------------------
# Add further table schemas here following the same pattern:
#
# _WALLET_SCHEMA = StructType([...])
# _PLAYERSESSION_SCHEMA = StructType([...])
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Central registry — the only place the silver script reads from
# ---------------------------------------------------------------------------
TABLE_CONFIGS: dict[str, TableConfig] = {
    "gametransaction": TableConfig(
        primary_key="gameTransactionId",
        schema=_GAMETRANSACTION_SCHEMA,
    ),
    # "wallet": TableConfig(primary_key="walletId", schema=_WALLET_SCHEMA),
    # "playersession": TableConfig(primary_key="playerSessionId", schema=_PLAYERSESSION_SCHEMA),
}


def get_table_config(table_name: str) -> TableConfig:
    """
    Return the TableConfig for *table_name*.

    Falls back to a minimal generic config for tables not yet explicitly
    defined (useful during incremental POC rollout of new tables).
    """
    if table_name in TABLE_CONFIGS:
        return TABLE_CONFIGS[table_name]

    # Generic fallback — covers remaining POC tables before full schema is defined
    return TableConfig(
        primary_key=f"{table_name}Id",
        schema=StructType(
            [
                StructField(f"{table_name}Id", StringType(), False),
                StructField("version", IntegerType(), True),
                StructField("createdAt", TimestampType(), True),
                StructField("updatedAt", TimestampType(), True),
            ]
        ),
    )
