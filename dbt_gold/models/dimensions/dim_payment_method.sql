/*
  dim_payment_method
  ==================
  Derived dimension — distinct (method, provider, gateway, type) combinations
  observed in silver.payment. No separate lookup table exists in MySQL;
  the dimension is derived directly from the fact source.

  Grain   : one row per (method, provider, gateway, type) combination
  Source  : silver.payment
  Used by : fact_payments (FK = sk_payment_method)

  Column groups:
    - Surrogate key  : dbt_utils.generate_surrogate_key on 4 natural key columns
    - Classification : method, type (deposit / withdrawal / refund)
    - Provider info  : provider name, gateway/PSP routing
*/

with source as (
    select * from {{ source('silver', 'payment') }}
),

distinct_methods as (
    select distinct
        method,
        provider,
        gateway,
        type
    from source
    where method   is not null
      and provider is not null
      and gateway  is not null
      and type     is not null
)

select
    -- -------------------------------------------------------------------------
    -- Surrogate key — dbt_utils handles NULL coalescing and adapter-specific hashing
    -- -------------------------------------------------------------------------
    {{ dbt_utils.generate_surrogate_key(['method', 'provider', 'gateway', 'type']) }}
                                as sk_payment_method,

    -- -------------------------------------------------------------------------
    -- Classification
    -- -------------------------------------------------------------------------
    method,                     -- WALLET / CARD / BANK / BANK_IBAN / VOUCHER …
    type,                       -- DEPOSIT / WITHDRAWAL / REFUND

    -- -------------------------------------------------------------------------
    -- Provider / routing info
    -- -------------------------------------------------------------------------
    provider,                   -- PAYPAL / KLARNA / PAYSAFECARD / NUVEI …
    gateway                     -- PAYMENT_IQ / SPORTRADAR …

from distinct_methods
