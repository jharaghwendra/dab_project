{{
    config(
        unique_key = 'sk_payment'
    )
}}

/*
  fact_payments
  =============
    Grain   : one row per payment_id (atomic payment event — no pre-aggregation)
    Source  : silver.ig_payment
  FKs     : sk_date_time → dim_date_time
            player_id   → dim_player
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
        payment_method,
        provider_name,
        gateway_name,
        payment_type
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
        from {{ source('silver', 'ig_payment') }}
        where status_code       = 'SUCCEEDED'
            and declined_deposit_id is null     -- exclude declined deposit retry rows
    {% if is_incremental() %}
    -- 2-day lookback on createdAt — safely captures payments created D-2 that
    -- transitioned to SUCCEEDED after the last pipeline run
    and created_at >= cast(date_format(date_sub(current_date(), 2), 'yyyy-MM-dd') as timestamp)
    {% endif %}
),

-- ---------------------------------------------------------------------------
-- Final: surrogate key + FK lookups + denormalized attributes
-- ---------------------------------------------------------------------------
final as (
    select
        -- Merge key (surrogate of natural PK + partition key)
        {{ dbt_utils.generate_surrogate_key(['s.payment_id', 's.country_code']) }}
                                                                        as sk_payment,

        -- Natural key (retain for traceability / dedup checks)
        s.payment_id,

        -- Time dimension FK (Vienna YYYYMMDDHH → joins to dim_date_time)
        s.sk_date_time,

        -- Player FK
        s.player_id,

        -- Partition / join key
        s.country_code,
        s.currency_code,

        -- Payment method FK + denormalized attributes (avoids join in Qlik QVD)
        pm.sk_payment_method,
        s.payment_type,
        s.payment_method,                    -- CARD / WALLET / BANK / VOUCHER …
        s.provider_name,                     -- PAYPAL / KLARNA / NUVEI …
        s.gateway_name,                      -- PAYMENT_IQ / SPORTRADAR …

        -- First-of-type flag (first deposit ever by this player)
        -- ByteType in source (0/1) — cast to int for clarity
        cast(coalesce(s.first_of_type, 0) as int) as is_first_deposit,

        -- ---------------------------------------------------------------
        -- Amounts (EUR normalised by silver pipeline)
        -- ---------------------------------------------------------------
        s.amount_eur,

        -- Fee charged by the PSP/gateway for this payment
        s.fee_eur,

        -- Net amount received after fee deduction
        ROUND(coalesce(s.amount_eur, 0) - coalesce(s.fee_eur, 0), 4)  as net_amount_eur,

        -- Amount in the customer-facing converted currency (for FX reporting)
        s.converted_amount_eur,
        s.converted_currency_code,

        -- ---------------------------------------------------------------
        -- Balance snapshots at time of payment (EUR normalised)
        -- Used by compliance / responsible gaming checks
        -- ---------------------------------------------------------------
        s.balance_before_eur,
        s.balance_after_eur,
        s.balance_before_real_eur,
        s.balance_after_real_eur,
        s.balance_before_bonus_eur,
        s.balance_after_bonus_eur,

        -- ---------------------------------------------------------------
        -- Raw timestamps (retained for debugging / SLA reporting)
        -- ---------------------------------------------------------------
        s.created_at,
        s.succeeded_at,

        -- Audit
        current_timestamp()  as last_update

    from source s
    left join dim_payment_method pm
        on  pm.payment_method = s.payment_method
        and pm.provider_name  = s.provider_name
        and pm.gateway_name   = s.gateway_name
        and pm.payment_type   = s.payment_type
)

select * from final
