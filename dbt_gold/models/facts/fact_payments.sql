{{
    config(
        unique_key = 'sk_payment'
    )
}}

/*
  fact_payments
  =============
  Grain   : one row per paymentId (atomic payment event — no pre-aggregation)
  Source  : silver.payment
  FKs     : sk_date_time → dim_date_time
            userId      → dim_player
            sk_payment_method → dim_payment_method

  Business rules:
    1. status = 'SUCCEEDED' only — excludes pending / failed / declined
    2. declinedDepositId IS NULL — critical: excludes declined deposit retry rows
       (a declined deposit can be retried with a new paymentId that carries the
        declinedDepositId of the original; including both would double-count the deposit)
    3. sk_date_time derived from succeededAt (when money actually moved), falling
       back to createdAt if succeededAt is NULL
    4. method/provider/gateway denormalized into fact for QlikView single-QVD load
       (avoids join at query time, consistent with fact_gametransaction_kpi pattern)

  Incremental strategy:
    - merge on sk_payment (surrogate of paymentId + country_code)
    - 2-day lookback on createdAt: catches late-arriving status updates (PENDING→SUCCEEDED)
    - full refresh available via --full-refresh if backfill needed
*/

-- ---------------------------------------------------------------------------
-- dim_payment_method: SK lookup for payment method classification
-- ---------------------------------------------------------------------------
with dim_payment_method as (
    select
        sk_payment_method,
        method,
        provider,
        gateway,
        type
    from {{ ref('dim_payment_method') }}
),

-- ---------------------------------------------------------------------------
-- Source: SUCCEEDED payments, excluding declined deposit retries
-- ---------------------------------------------------------------------------
source as (
    select
        *,
        -- sk_date_time: Vienna local YYYYMMDDHH — use succeededAt (revenue recognition
        -- timestamp) with createdAt fallback for payments that lack a succeededAt
        cast(
            year(from_utc_timestamp(coalesce(succeededAt, createdAt), 'Europe/Vienna'))    * 1000000
            + month(from_utc_timestamp(coalesce(succeededAt, createdAt), 'Europe/Vienna')) * 10000
            + day(from_utc_timestamp(coalesce(succeededAt, createdAt), 'Europe/Vienna'))   * 100
            + hour(from_utc_timestamp(coalesce(succeededAt, createdAt), 'Europe/Vienna'))
        as int)  as sk_date_time
    from {{ source('silver', 'payment') }}
    where status             = 'SUCCEEDED'
      and declinedDepositId  is null     -- exclude declined deposit retry rows
    {% if is_incremental() %}
    -- 2-day lookback on createdAt — safely captures payments created D-2 that
    -- transitioned to SUCCEEDED after the last pipeline run
    and createdAt >= cast(date_format(date_sub(current_date(), 2), 'yyyy-MM-dd') as timestamp)
    {% endif %}
),

-- ---------------------------------------------------------------------------
-- Final: surrogate key + FK lookups + denormalized attributes
-- ---------------------------------------------------------------------------
final as (
    select
        -- Merge key (surrogate of natural PK + partition key)
        {{ dbt_utils.generate_surrogate_key(['s.paymentId', 's.country_code']) }}
                                                                        as sk_payment,

        -- Natural key (retain for traceability / dedup checks)
        s.paymentId,

        -- Time dimension FK (Vienna YYYYMMDDHH → joins to dim_date_time)
        s.sk_date_time,

        -- Player FK
        s.userId,

        -- Partition / join key
        s.country_code,
        s.currencyCode,

        -- Payment method FK + denormalized attributes (avoids join in Qlik QVD)
        pm.sk_payment_method,
        s.type          as payment_type,     -- DEPOSIT / WITHDRAWAL / REFUND
        s.method,                            -- CARD / WALLET / BANK / VOUCHER …
        s.provider,                          -- PAYPAL / KLARNA / NUVEI …
        s.gateway,                           -- PAYMENT_IQ / SPORTRADAR …

        -- First-of-type flag (first deposit ever by this player)
        -- ByteType in source (0/1) — cast to int for clarity
        cast(coalesce(s.firstOfType, 0) as int)  as is_first_deposit,

        -- ---------------------------------------------------------------
        -- Amounts (EUR normalised by silver pipeline)
        -- ---------------------------------------------------------------
        s.amount_eur,

        -- Fee charged by the PSP/gateway for this payment
        s.fee_eur,

        -- Net amount received after fee deduction
        ROUND(coalesce(s.amount_eur, 0) - coalesce(s.fee_eur, 0), 4)  as net_amount_eur,

        -- Amount in the customer-facing converted currency (for FX reporting)
        s.convertedAmount_eur,
        s.convertedCurrencyCode,

        -- ---------------------------------------------------------------
        -- Balance snapshots at time of payment (EUR normalised)
        -- Used by compliance / responsible gaming checks
        -- ---------------------------------------------------------------
        s.balanceBefore_eur,
        s.balanceAfter_eur,
        s.balanceBeforeReal_eur,
        s.balanceAfterReal_eur,
        s.balanceBeforeBonus_eur,
        s.balanceAfterBonus_eur,

        -- ---------------------------------------------------------------
        -- Raw timestamps (retained for debugging / SLA reporting)
        -- ---------------------------------------------------------------
        s.createdAt,
        s.succeededAt,

        -- Audit
        current_timestamp()  as last_update

    from source s
    left join dim_payment_method pm
        on  pm.method   = s.method
        and pm.provider = s.provider
        and pm.gateway  = s.gateway
        and pm.type     = s.type
)

select * from final
