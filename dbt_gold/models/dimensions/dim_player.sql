/*
  dim_player
  ==========
  SCD Type 1 — one row per player, always reflecting the latest state.
  Source : tma_dev.silver.userdata (pk = userId)

  Column groups:
    - Identity       : who the player is and which brand/org they belong to
    - Profile        : demographic and location attributes
    - Registration   : key lifecycle timestamps (when they registered, first bet, last login)
    - KYC/Compliance : verification status, risk flags, regulatory checks
    - Account state  : lock status, VIP level, migration flag
    - Metadata       : record freshness tracking
*/

with source as (
    select * from {{ source('silver', 'userdata') }}
)

select

    -- -------------------------------------------------------------------------
    -- Identity
    -- -------------------------------------------------------------------------
    userId,
    brandId,
    organizationId,
    jurisdiction,

    -- -------------------------------------------------------------------------
    -- Profile — demographic and location attributes
    -- -------------------------------------------------------------------------
    firstName,
    lastName,
    gender,
    birthDate,
    nationality,
    countryCode,
    localeCode,
    currencyCode,
    city,
    postcode,
    state,

    -- -------------------------------------------------------------------------
    -- Registration / lifecycle timestamps
    -- -------------------------------------------------------------------------
    createdAt          as registeredAt,
    firstDepositAt,
    firstBetAt,
    firstRealMoneyBetAt,
    lastLoginAt,
    lastBetAt,

    -- -------------------------------------------------------------------------
    -- KYC / Compliance — verification and risk flags
    -- -------------------------------------------------------------------------
    verifiedKyc,
    verifiedStatus,
    schufaKycRisk,
    pep,                    -- Politically Exposed Person flag
    sanctioned,
    suspectedFraud,
    retailVerifiedAt,
    sumsubKycVerifiedAt,

    -- -------------------------------------------------------------------------
    -- Account state
    -- -------------------------------------------------------------------------
    locked,
    vipLevel,
    isMigrated,
    isEmailVerified,

    -- -------------------------------------------------------------------------
    -- Metadata — record freshness
    -- -------------------------------------------------------------------------
    updatedAt           as playerUpdatedAt

from source
