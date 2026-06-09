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
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class TableConfig:
    primary_key: str
    schema: StructType
    version_col: str = "version"  # column used to pick the latest row per primary key in micro-batch dedup


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
# userdata
# ---------------------------------------------------------------------------
# MySQL indexes (reference for Delta Z-ORDER / bloom filter optimisation):
#   idx_createdat       (createdAt)
#   idx_firstDepositAt  (firstDepositAt)
#   idx_last_update     (last_update)
#   idx_updatedat       (updatedAt)
_USERDATA_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("achievementLevel", IntegerType(), True),
        StructField("address1", StringType(), True),
        StructField("address2", StringType(), True),
        StructField("affiliateId", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("balances", StringType(), True),
        StructField("betLimitActive", ByteType(), True),
        StructField("betLimitRemaining", DecimalType(34, 4), True),
        StructField("bgw", DecimalType(34, 4), True),
        StructField("birthDate", DateType(), True),
        StructField("birthCity", StringType(), True),
        StructField("birthDay", DateType(), True),
        StructField("bonusCost", DecimalType(34, 4), True),
        StructField("brandId", StringType(), True),
        StructField("city", StringType(), True),
        StructField("countryCode", StringType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("definiteLockActive", ByteType(), True),
        StructField("depositLimitActive", ByteType(), True),
        StructField("depositLimitRemaining", DecimalType(34, 4), True),
        StructField("email", StringType(), True),
        StructField("firstBetAt", TimestampType(), True),
        StructField("firstDepositAmount", DecimalType(34, 4), True),
        StructField("firstDepositAt", TimestampType(), True),
        StructField("firstDepositCurrencyCode", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("firstRealMoneyBetAt", TimestampType(), True),
        StructField("gender", StringType(), True),
        StructField("ggw", DecimalType(34, 4), True),
        StructField("indefiniteLockActive", ByteType(), True),
        StructField("ip", StringType(), True),
        StructField("isEmailVerified", ByteType(), True),
        StructField("isMigrated", ByteType(), True),
        StructField("isPhoneVerified", ByteType(), True),
        StructField("jurisdiction", StringType(), True),
        StructField("lastBetAt", TimestampType(), True),
        StructField("lastBetLimitExceededAt", TimestampType(), True),
        StructField("lastDepositAmount", DecimalType(34, 4), True),
        StructField("lastDepositAt", TimestampType(), True),
        StructField("lastDepositLimitExceededAt", TimestampType(), True),
        StructField("lastDepositCurrencyCode", StringType(), True),
        StructField("lastLockAt", TimestampType(), True),
        StructField("lastLockReason", StringType(), True),
        StructField("lastLoginAt", TimestampType(), True),
        StructField("lastLossLimitExceededAt", TimestampType(), True),
        StructField("lastName", StringType(), True),
        StructField("lastRealMoneyBetAt", TimestampType(), True),
        StructField("lastSessionLimitExceededAt", TimestampType(), True),
        StructField("localeCode", StringType(), True),
        StructField("locked", ByteType(), True),
        StructField("loginAfterLock", ByteType(), True),
        StructField("lossLimitActive", ByteType(), True),
        StructField("lossLimitRemaining", DecimalType(34, 4), True),
        StructField("maidenName", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("numberOfDeposits", IntegerType(), True),
        StructField("numberOfRefunds", IntegerType(), True),
        StructField("numberOfWithdrawals", IntegerType(), True),
        StructField("organizationId", StringType(), True),
        StructField("pathProgress", StringType(), True),
        StructField("pep", ByteType(), True),
        StructField("phone", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("receiveEmail", ByteType(), True),
        StructField("receivePhone", ByteType(), True),
        StructField("receiveSms", ByteType(), True),
        StructField("receiveSnail", ByteType(), True),
        StructField("referralUri", StringType(), True),
        StructField("riskTags", StringType(), True),
        StructField("rgw", DecimalType(34, 4), True),
        StructField("sanctioned", ByteType(), True),
        StructField("segmentIds", StringType(), True),
        StructField("sessionLimitActive", ByteType(), True),
        StructField("sessionLimitRemaining", DoubleType(), True),
        StructField("state", StringType(), True),
        StructField("suspectedFraud", ByteType(), True),
        StructField("tags", StringType(), True),
        StructField("totalBalance", DecimalType(34, 4), True),
        StructField("totalBet", DecimalType(34, 4), True),
        StructField("totalBetSinceLastDeposit", DecimalType(34, 4), True),
        StructField("totalDeposit", DecimalType(34, 4), True),
        StructField("totalDepositInCurrency", StringType(), True),
        StructField("totalRealBetSinceLastDeposit", DecimalType(34, 4), True),
        StructField("totalRefund", DecimalType(34, 4), True),
        StructField("totalRefundInCurrency", StringType(), True),
        StructField("totalWin", DecimalType(34, 4), True),
        StructField("totalWithdraw", DecimalType(34, 4), True),
        StructField("userId", StringType(), False),
        StructField("verifiedKyc", ByteType(), True),
        StructField("vipLevel", IntegerType(), True),
        StructField("browser", StringType(), True),
        StructField("device", StringType(), True),
        StructField("os", StringType(), True),
        StructField("bgwInCurrency", StringType(), True),
        StructField("bonusCostInCurrency", StringType(), True),
        StructField("firstRealMoneyBetAtCurrencyCode", StringType(), True),
        StructField("ggwInCurrency", StringType(), True),
        StructField("lastBetAtCurrencyCode", StringType(), True),
        StructField("lastRealMoneyBetAtCurrencyCode", StringType(), True),
        StructField("retailVerifiedAt", TimestampType(), True),
        StructField("sumsubKycVerifiedAt", TimestampType(), True),
        StructField("totalBetInCurrency", StringType(), True),
        StructField("totalBetSinceLastDepositInCurrency", StringType(), True),
        StructField("totalBonusBalance", DecimalType(34, 4), True),
        StructField("totalBonusBalanceInCurrency", StringType(), True),
        StructField("totalBonusBet", DecimalType(34, 4), True),
        StructField("totalBonusBetInCurrency", StringType(), True),
        StructField("totalBonusWin", DecimalType(34, 4), True),
        StructField("totalBonusWinInCurrency", StringType(), True),
        StructField("totalDepositForLicence", DecimalType(34, 4), True),
        StructField("totalRealBalance", DecimalType(34, 4), True),
        StructField("totalRealBalanceInCurrency", StringType(), True),
        StructField("totalRealBet", DecimalType(34, 4), True),
        StructField("totalRealBetInCurrency", StringType(), True),
        StructField("totalRealBetSinceLastDepositInCurrency", StringType(), True),
        StructField("totalRealWin", DecimalType(34, 4), True),
        StructField("totalRealWinInCurrency", StringType(), True),
        StructField("totalRefundForLicence", DecimalType(34, 4), True),
        StructField("totalWinInCurrency", StringType(), True),
        StructField("totalWithdrawForLicence", DecimalType(34, 4), True),
        StructField("totalWithdrawInCurrency", StringType(), True),
        StructField("firstBetAtCurrencyCode", StringType(), True),
        StructField("lastNameAffix", StringType(), True),
        StructField("lastNetDepositLimitExceededAt", TimestampType(), True),
        StructField("schufaKycRisk", StringType(), True),
        StructField("verifiedStatus", StringType(), True),
        StructField("discardable", ByteType(), True),
        StructField("betLimitRemaining_eur", DecimalType(34, 4), True),
        StructField("betLimitRemaining_base", DecimalType(34, 4), True),
        StructField("bgw_eur", DecimalType(34, 4), True),
        StructField("bgw_base", DecimalType(34, 4), True),
        StructField("bonusCost_eur", DecimalType(34, 4), True),
        StructField("bonusCost_base", DecimalType(34, 4), True),
        StructField("depositLimitRemaining_eur", DecimalType(34, 4), True),
        StructField("depositLimitRemaining_base", DecimalType(34, 4), True),
        StructField("firstDepositAmount_eur", DecimalType(34, 4), True),
        StructField("firstDepositAmount_base", DecimalType(34, 4), True),
        StructField("ggw_eur", DecimalType(34, 4), True),
        StructField("ggw_base", DecimalType(34, 4), True),
        StructField("lastDepositAmount_eur", DecimalType(34, 4), True),
        StructField("lastDepositAmount_base", DecimalType(34, 4), True),
        StructField("lossLimitRemaining_eur", DecimalType(34, 4), True),
        StructField("lossLimitRemaining_base", DecimalType(34, 4), True),
        StructField("rgw_eur", DecimalType(34, 4), True),
        StructField("rgw_base", DecimalType(34, 4), True),
        StructField("totalBalance_eur", DecimalType(34, 4), True),
        StructField("totalBalance_base", DecimalType(34, 4), True),
        StructField("totalBet_eur", DecimalType(34, 4), True),
        StructField("totalBet_base", DecimalType(34, 4), True),
        StructField("totalBetSinceLastDeposit_eur", DecimalType(34, 4), True),
        StructField("totalBetSinceLastDeposit_base", DecimalType(34, 4), True),
        StructField("totalBonusBalance_eur", DecimalType(34, 4), True),
        StructField("totalBonusBalance_base", DecimalType(34, 4), True),
        StructField("totalBonusBet_eur", DecimalType(34, 4), True),
        StructField("totalBonusBet_base", DecimalType(34, 4), True),
        StructField("totalBonusWin_eur", DecimalType(34, 4), True),
        StructField("totalBonusWin_base", DecimalType(34, 4), True),
        StructField("totalDeposit_eur", DecimalType(34, 4), True),
        StructField("totalDeposit_base", DecimalType(34, 4), True),
        StructField("totalDepositForLicence_eur", DecimalType(34, 4), True),
        StructField("totalDepositForLicence_base", DecimalType(34, 4), True),
        StructField("totalRealBalance_eur", DecimalType(34, 4), True),
        StructField("totalRealBalance_base", DecimalType(34, 4), True),
        StructField("totalRealBet_eur", DecimalType(34, 4), True),
        StructField("totalRealBet_base", DecimalType(34, 4), True),
        StructField("totalRealBetSinceLastDeposit_eur", DecimalType(34, 4), True),
        StructField("totalRealBetSinceLastDeposit_base", DecimalType(34, 4), True),
        StructField("totalRealWin_eur", DecimalType(34, 4), True),
        StructField("totalRealWin_base", DecimalType(34, 4), True),
        StructField("totalRefund_eur", DecimalType(34, 4), True),
        StructField("totalRefund_base", DecimalType(34, 4), True),
        StructField("totalRefundForLicence_eur", DecimalType(34, 4), True),
        StructField("totalRefundForLicence_base", DecimalType(34, 4), True),
        StructField("totalWin_eur", DecimalType(34, 4), True),
        StructField("totalWin_base", DecimalType(34, 4), True),
        StructField("totalWithdraw_eur", DecimalType(34, 4), True),
        StructField("totalWithdraw_base", DecimalType(34, 4), True),
        StructField("totalWithdrawForLicence_eur", DecimalType(34, 4), True),
        StructField("totalWithdrawForLicence_base", DecimalType(34, 4), True),
        StructField("firstOnlineSessionAt", TimestampType(), True),
        StructField("betLimitTotal", DecimalType(34, 4), True),
        StructField("depositLimitTotal", DecimalType(34, 4), True),
        StructField("lossLimitTotal", DecimalType(34, 4), True),
        StructField("mgtKycVerifiedAt", TimestampType(), True),
        StructField("multiLog24VerifiedAt", TimestampType(), True),
        StructField("sessionLimitTotal", DecimalType(34, 4), True),
        StructField("shopRef", StringType(), True),
        StructField("terminalRef", StringType(), True),
        StructField("type", StringType(), True),
        StructField("betLimitTotal_eur", DecimalType(34, 4), True),
        StructField("betLimitTotal_base", DecimalType(34, 4), True),
        StructField("depositLimitTotal_eur", DecimalType(34, 4), True),
        StructField("depositLimitTotal_base", DecimalType(34, 4), True),
        StructField("lossLimitTotal_eur", DecimalType(34, 4), True),
        StructField("lossLimitTotal_base", DecimalType(34, 4), True),
    ]
)

# ---------------------------------------------------------------------------
# payment
# ---------------------------------------------------------------------------
# MySQL indexes (reference for Delta Z-ORDER / bloom filter optimisation):
#   idx_completedat_type            (completedAt, type)
#   idx_createdat                   (createdAt)
#   idx_last_update                 (last_update)
#   idx_shopref_type_succeededat    (shopRef(10), type, succeededAt)
#   idx_succeededat_type_provider   (succeededAt, type, provider)
#   idx_terminalRef_type_succeededat(terminalRef(10), type, succeededAt)
#   idx_updatedat                   (updatedAt)
#   idx_userid                      (userId(20))
_PAYMENT_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("balanceAfter", DecimalType(34, 4), True),
        StructField("balanceAfterBonus", DecimalType(34, 4), True),
        StructField("balanceAfterReal", DecimalType(34, 4), True),
        StructField("balanceBefore", DecimalType(34, 4), True),
        StructField("balanceBeforeBonus", DecimalType(34, 4), True),
        StructField("balanceBeforeReal", DecimalType(34, 4), True),
        StructField("balanceCurrencyCode", StringType(), True),
        StructField("balancesBefore", StringType(), True),  # json
        StructField("balancesAfter", StringType(), True),  # json
        StructField("succeededAt", TimestampType(), True),
        StructField("accountId", StringType(), True),
        StructField("accountRef", StringType(), True),
        StructField("amount", DecimalType(34, 4), True),
        StructField("approverIds", StringType(), True),  # json
        StructField("brandId", StringType(), True),
        StructField("convertedAmount", DecimalType(34, 4), True),
        StructField("convertedCurrencyCode", StringType(), True),
        StructField("convertedFee", DecimalType(34, 4), True),
        StructField("countryCode", StringType(), True),
        StructField("creatorId", StringType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("declineReason", StringType(), True),
        StructField("declinedDepositId", StringType(), True),
        StructField("device", StringType(), True),
        StructField("errorMessage", StringType(), True),
        StructField("fee", DecimalType(34, 4), True),
        StructField("firstOfType", ByteType(), True),
        StructField("gateway", StringType(), True),
        StructField("gatewayAccount", StringType(), True),
        StructField("initialProvider", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("localeCode", StringType(), True),
        StructField("method", StringType(), True),
        StructField("paymentId", StringType(), False),  # PK
        StructField("provider", StringType(), True),
        StructField("pspRef", StringType(), True),
        StructField("rate", DoubleType(), True),
        StructField("redirectUri", StringType(), True),
        StructField("ref", StringType(), True),
        StructField("relatedPaymentId", StringType(), True),
        StructField("selectedBonusCode", StringType(), True),
        StructField("sessionId", StringType(), True),
        StructField("status", StringType(), True),
        StructField("statusCode", StringType(), True),
        StructField("type", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
        StructField("vipLevel", IntegerType(), True),
        StructField("version", IntegerType(), True),
        StructField("completedAt", TimestampType(), True),
        StructField("links", StringType(), True),  # json
        StructField("returnedById", StringType(), True),
        StructField("shopRef", StringType(), True),
        StructField("terminalRef", StringType(), True),
        StructField("membercardNumber", StringType(), True),
        StructField("migrationId", StringType(), True),
        StructField("selectedShopItemIds", StringType(), True),
        StructField("discardable", ByteType(), True),
        StructField("amount_eur", DecimalType(34, 4), True),
        StructField("amount_base", DecimalType(34, 4), True),
        StructField("balanceAfter_eur", DecimalType(34, 4), True),
        StructField("balanceAfter_base", DecimalType(34, 4), True),
        StructField("balanceAfterBonus_eur", DecimalType(34, 4), True),
        StructField("balanceAfterBonus_base", DecimalType(34, 4), True),
        StructField("balanceAfterReal_eur", DecimalType(34, 4), True),
        StructField("balanceAfterReal_base", DecimalType(34, 4), True),
        StructField("balanceBefore_eur", DecimalType(34, 4), True),
        StructField("balanceBefore_base", DecimalType(34, 4), True),
        StructField("balanceBeforeBonus_eur", DecimalType(34, 4), True),
        StructField("balanceBeforeBonus_base", DecimalType(34, 4), True),
        StructField("balanceBeforeReal_eur", DecimalType(34, 4), True),
        StructField("balanceBeforeReal_base", DecimalType(34, 4), True),
        StructField("convertedAmount_eur", DecimalType(34, 4), True),
        StructField("convertedAmount_base", DecimalType(34, 4), True),
        StructField("convertedFee_eur", DecimalType(34, 4), True),
        StructField("convertedFee_base", DecimalType(34, 4), True),
        StructField("fee_eur", DecimalType(34, 4), True),
        StructField("fee_base", DecimalType(34, 4), True),
        StructField("last_update", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------
# MySQL indexes (reference for Delta Z-ORDER / bloom filter optimisation):
#   idx_createdat              (createdAt)
#   idx_last_update            (last_update)
#   idx_shopRef_createdat      (shopRef(6), createdAt)
#   idx_updatedat              (updatedAt)
# Talend upsert key: updatedAt — max(updatedAt) per checkId, srct.updatedAt > ch.updatedAt
_CHECK_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
        StructField("brandId", StringType(), True),
        StructField("checkId", StringType(), False),  # PK
        StructField("createdAt", TimestampType(), True),
        StructField("details", StringType(), True),  # json
        StructField("event", StringType(), True),
        StructField("eventRef", StringType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("provider", StringType(), True),
        StructField("ref", StringType(), True),
        StructField("risk", StringType(), True),
        StructField("shopRef", StringType(), True),
        StructField("status", StringType(), True),
        StructField("type", StringType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("userId", StringType(), True),
        StructField("discardable", ByteType(), True),
        StructField("creatorId", StringType(), True),
        StructField("last_update", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# gameround
# ---------------------------------------------------------------------------
# MySQL indexes (reference for Delta Z-ORDER / bloom filter optimisation):
#   idx_createdat    (createdAt)
#   idx_last_update  (last_update)
#   idx_updatedat    (updatedAt)
#   idx_version      (version)
# Talend upsert key: version — MAX(version) per gameRoundId, srct.version > gr.version
_GAMEROUND_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("availableBalancesAfter", StringType(), True),  # json
        StructField("availableBalancesBefore", StringType(), True),  # json
        StructField("balancesAfter", StringType(), True),  # json
        StructField("balancesBefore", StringType(), True),  # json
        StructField("betAmount", DecimalType(34, 4), True),
        StructField("betCount", IntegerType(), True),
        StructField("bonusBetAmount", DecimalType(34, 4), True),
        StructField("bonusWinAmount", DecimalType(34, 4), True),
        StructField("brandId", StringType(), True),
        StructField("closed", ByteType(), True),
        StructField("currencyCode", StringType(), True),
        StructField("details", StringType(), True),  # json
        StructField("finishedAt", TimestampType(), True),
        StructField("gameId", StringType(), True),
        StructField("gameRoundId", StringType(), False),  # PK
        StructField("gameSessionId", StringType(), True),
        StructField("manuallyClosed", ByteType(), True),
        StructField("promotional", ByteType(), True),
        StructField("provider", StringType(), True),
        StructField("providerGameRoundId", StringType(), True),
        StructField("providerGameSessionId", StringType(), True),
        StructField("studio", StringType(), True),
        StructField("syndicateSessionId", StringType(), True),
        StructField("taxAmount", DecimalType(34, 4), True),
        StructField("totalBalance", DecimalType(34, 4), True),
        StructField("userId", StringType(), True),
        StructField("walletCurrencyCode", StringType(), True),
        StructField("winAmount", DecimalType(34, 4), True),
        StructField("winBet", DecimalType(34, 4), True),
        StructField("winCount", IntegerType(), True),
        StructField("jackpotAmount", StringType(), True),  # stored as text in MySQL
        StructField("jackpotContribution", StringType(), True),  # stored as text in MySQL
        StructField("shopRef", StringType(), True),
        StructField("terminalRef", StringType(), True),
        StructField("discardable", ByteType(), True),
        StructField("betAmount_eur", DecimalType(34, 4), True),
        StructField("betAmount_base", DecimalType(34, 4), True),
        StructField("bonusBetAmount_eur", DecimalType(34, 4), True),
        StructField("bonusBetAmount_base", DecimalType(34, 4), True),
        StructField("bonusWinAmount_eur", DecimalType(34, 4), True),
        StructField("bonusWinAmount_base", DecimalType(34, 4), True),
        StructField("taxAmount_eur", DecimalType(34, 4), True),
        StructField("taxAmount_base", DecimalType(34, 4), True),
        StructField("totalBalance_eur", DecimalType(34, 4), True),
        StructField("totalBalance_base", DecimalType(34, 4), True),
        StructField("winAmount_eur", DecimalType(34, 4), True),
        StructField("winAmount_base", DecimalType(34, 4), True),
        StructField("version", IntegerType(), True),
        StructField("last_update", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Add further table schemas here following the same pattern:
#
# _WALLET_SCHEMA = StructType([...])
# _PLAYERSESSION_SCHEMA = StructType([...])
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# game
# ---------------------------------------------------------------------------
# MySQL indexes: idx_createdat (createdAt), idx_updatedat (updatedAt), idx_last_update (last_update)
# Upsert key: version (text in MySQL but treated as dedup signal — updatedAt used instead)
_GAME_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("allowedWalletType", StringType(), True),
        StructField("bonusGame", ByteType(), True),
        StructField("bonusHitFrequency", StringType(), True),
        StructField("branded", ByteType(), True),
        StructField("class", StringType(), True),
        StructField("enabled", ByteType(), True),
        StructField("features", StringType(), True),  # JSON stored as string
        StructField("freeGames", ByteType(), True),
        StructField("gamble", ByteType(), True),
        StructField("gameId", StringType(), False),  # PK
        StructField("height", IntegerType(), True),
        StructField("jackpotId", StringType(), True),
        StructField("layout", StringType(), True),
        StructField("live", ByteType(), True),
        StructField("loginRequired", ByteType(), True),
        StructField("maxBet", StringType(), True),
        StructField("maxCoins", IntegerType(), True),
        StructField("maxLines", StringType(), True),
        StructField("maxPayout", StringType(), True),
        StructField("minBet", StringType(), True),
        StructField("name", StringType(), True),
        StructField("progressiveJackpot", ByteType(), True),
        StructField("reelsRows", StringType(), True),
        StructField("releasedAt", TimestampType(), True),
        StructField("restrictedCountries", StringType(), True),  # JSON stored as string
        StructField("restrictedJurisdictions", StringType(), True),  # JSON stored as string
        StructField("rtp", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("studio", StringType(), True),
        StructField("tableId", StringType(), True),
        StructField("tags", StringType(), True),  # JSON stored as string
        StructField("tcUpdatedAt", TimestampType(), True),
        StructField("type", StringType(), True),
        StructField("version", StringType(), True),  # text in MySQL — not an int version
        StructField("vertical", StringType(), True),
        StructField("volatility", StringType(), True),
        StructField("wageringCoefficient", StringType(), True),
        StructField("width", IntegerType(), True),
        StructField("complianceDetails", StringType(), True),
        StructField("freeGamesFixedAmount", ByteType(), True),
        StructField("private", ByteType(), True),
        StructField("videoThumbnail", ByteType(), True),
        StructField("discardable", ByteType(), True),
        StructField("last_update", TimestampType(), True),
    ]
)


# ---------------------------------------------------------------------------
# brandgame
# ---------------------------------------------------------------------------
# Brand-specific configuration layer on top of game.
# One brandgame row per (brand, game) combination.
# brandgame.gameId (text FK) → game.gameId
# MySQL indexes: idx_createdat (createdAt), idx_updatedat (updatedAt), idx_last_update (last_update)
_BRANDGAME_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("allowedWalletType", StringType(), True),
        StructField("bonusGame", ByteType(), True),
        StructField("bonusHitFrequency", StringType(), True),
        StructField("branded", ByteType(), True),
        StructField("class", StringType(), True),
        StructField("enabled", ByteType(), True),
        StructField("features", StringType(), True),  # JSON stored as string
        StructField("freeGames", ByteType(), True),
        StructField("gamble", ByteType(), True),
        StructField("brandGameId", StringType(), False),  # PK
        StructField("height", IntegerType(), True),
        StructField("jackpotId", StringType(), True),
        StructField("layout", StringType(), True),
        StructField("live", ByteType(), True),
        StructField("loginRequired", ByteType(), True),
        StructField("maxBet", StringType(), True),
        StructField("maxCoins", IntegerType(), True),
        StructField("maxLines", StringType(), True),
        StructField("maxPayout", StringType(), True),
        StructField("minBet", StringType(), True),
        StructField("name", StringType(), True),
        StructField("progressiveJackpot", ByteType(), True),
        StructField("reelsRows", StringType(), True),
        StructField("releasedAt", TimestampType(), True),
        StructField("restrictedCountries", StringType(), True),  # JSON stored as string
        StructField("restrictedJurisdictions", StringType(), True),  # JSON stored as string
        StructField("rtp", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("studio", StringType(), True),
        StructField("tableId", StringType(), True),
        StructField("tags", StringType(), True),  # JSON stored as string
        StructField("tcUpdatedAt", TimestampType(), True),
        StructField("type", StringType(), True),
        StructField("version", StringType(), True),  # text in MySQL — not an int version
        StructField("vertical", StringType(), True),
        StructField("volatility", StringType(), True),
        StructField("wageringCoefficient", StringType(), True),
        StructField("width", IntegerType(), True),
        StructField("enabledAt", TimestampType(), True),
        StructField("disabledAt", TimestampType(), True),
        StructField("firstEnabledAt", TimestampType(), True),
        StructField("complianceDetails", StringType(), True),
        StructField("freeGamesFixedAmount", ByteType(), True),
        StructField("gameEnabled", ByteType(), True),
        StructField("gameId", StringType(), True),  # FK → game.gameId
        StructField("gameProviderId", StringType(), True),
        StructField("gameRestrictedCountries", StringType(), True),
        StructField("gameRestrictedJurisdictions", StringType(), True),
        StructField("gameSettingsAvailable", ByteType(), True),
        StructField("jurisdictionRtpLevels", StringType(), True),
        StructField("privateStatus", StringType(), True),
        StructField("resellerOrganizationId", StringType(), True),
        StructField("discardable", ByteType(), True),
        StructField("assets", StringType(), True),
        StructField("last_update", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# tag
# ---------------------------------------------------------------------------
# MySQL indexes (reference for Delta Z-ORDER / bloom filter optimisation):
#   idx_createdat    (createdAt)
#   idx_updatedat    (updatedAt)
#   idx_targetId     (targetId(34))
# PK: tagId (varchar 255)
# version_col: version — used for dedup ordering in merge
# Key use: test-user exclusion filter — rows where targetType='User' AND tagCategory='TEST'
# Schema verified against actual S3 parquet export (16,918 files, Jan–Jun 2026):
#   active=bool, discardable=bool, version=int64 — brandId and last_update NOT in S3 export
_TAG_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("active", BooleanType(), True),  # bool in parquet (not tinyint)
        StructField("creatorId", StringType(), True),
        StructField("tagCategory", StringType(), True),  # e.g. 'TEST', 'VIP', 'FRAUD'
        StructField("tagId", StringType(), False),  # PK — varchar(255)
        StructField("targetId", StringType(), True),  # userId when targetType='User'
        StructField("targetType", StringType(), True),  # e.g. 'User'
        StructField("updaterId", StringType(), True),
        StructField("discardable", BooleanType(), True),  # bool in parquet (not tinyint)
        StructField("version", LongType(), True),  # int64 in parquet
    ]
)

# ---------------------------------------------------------------------------
# userlimit
# ---------------------------------------------------------------------------
# MySQL indexes (reference for Delta Z-ORDER / bloom filter optimisation):
#   idx_last_update   (last_update)
#   idx_createdat     (createdAt)
#   idx_updatedat     (updatedAt)
#   idx_userid        (userId(20))
#   idx_version       (version)
# Talend upsert key: version — version-based merge (same pattern as gametransaction)
# SCD2 target: dbt snapshot snap_userlimit uses updatedAt as strategy=timestamp
_USERLIMIT_SCHEMA = StructType(
    [
        StructField("createdAt", TimestampType(), True),
        StructField("updatedAt", TimestampType(), True),
        StructField("processedAt", TimestampType(), True),
        StructField("type", StringType(), True),  # DEPOSIT / LOSS / WAGER / SESSION_DURATION …
        StructField("userId", StringType(), True),
        StructField("status", StringType(), True),  # ACTIVE / CANCELED / PENDING
        StructField("value", DecimalType(34, 4), True),
        StructField("value_eur", DecimalType(34, 4), True),
        StructField("value_base", DecimalType(34, 4), True),
        StructField("currencyCode", StringType(), True),
        StructField("brandId", StringType(), True),
        StructField("jurisdiction", StringType(), True),
        StructField("period", StringType(), True),  # DAILY / WEEKLY / MONTHLY …
        StructField("activeFrom", TimestampType(), True),
        StructField("activeUntil", TimestampType(), True),
        StructField("nextResetTime", TimestampType(), True),
        StructField("previousLimitValue", DecimalType(34, 4), True),  # value before last change
        StructField("progress", StringType(), True),  # JSON — current progress toward limit
        StructField("userLimitId", StringType(), False),  # PK — varchar(255)
        StructField("creatorId", StringType(), True),
        StructField("migrationId", StringType(), True),
        StructField("discardable", ByteType(), True),
        StructField("version", IntegerType(), True),
        StructField("cancelingUserLimitRequestId", StringType(), True),
        StructField("creatingUserLimitRequestId", StringType(), True),
        StructField("last_update", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Central registry — the only place the silver script reads from
# ---------------------------------------------------------------------------
TABLE_CONFIGS: dict[str, TableConfig] = {
    "gametransaction": TableConfig(
        primary_key="gameTransactionId",
        schema=_GAMETRANSACTION_SCHEMA,
        version_col="version",  # int version column — used for dedup ordering and merge condition
    ),
    "userdata": TableConfig(
        primary_key="userId",
        schema=_USERDATA_SCHEMA,
        version_col="updatedAt",  # userdata has no version int; use updatedAt timestamp for dedup ordering
    ),
    "payment": TableConfig(
        primary_key="paymentId",
        schema=_PAYMENT_SCHEMA,
        version_col="version",  # int version column — same pattern as gametransaction
    ),
    "check": TableConfig(
        primary_key="checkId",
        schema=_CHECK_SCHEMA,
        version_col="updatedAt",  # no version int; use updatedAt timestamp — same pattern as userdata
    ),
    "gameround": TableConfig(
        primary_key="gameRoundId",
        schema=_GAMEROUND_SCHEMA,
        version_col="version",  # int version column — same pattern as gametransaction
    ),
    "game": TableConfig(
        primary_key="gameId",
        schema=_GAME_SCHEMA,
        version_col="updatedAt",  # version col in MySQL is text (not int) — use updatedAt for dedup ordering
    ),
    "brandgame": TableConfig(
        primary_key="brandGameId",
        schema=_BRANDGAME_SCHEMA,
        version_col="updatedAt",  # version col is text (not int) — use updatedAt for dedup ordering
    ),
    "tag": TableConfig(
        primary_key="tagId",
        schema=_TAG_SCHEMA,
        version_col="version",  # int version column — used for dedup ordering and merge condition
    ),
    "userlimit": TableConfig(
        primary_key="userLimitId",
        schema=_USERLIMIT_SCHEMA,
        version_col="version",  # int version column — same pattern as gametransaction
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
