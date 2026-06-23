/*
  dim_payment_method
  ==================
  Derived dimension — distinct (payment_method, provider_name, gateway_name, payment_type) combinations
    observed in silver.ig_payment. No separate lookup table exists upstream;
  the dimension is derived directly from the fact source.

  Grain   : one row per (method, provider, gateway, type) combination
  Source  : silver.ig_payment
  Used by : fact_payments (FK = sk_payment_method)

  Column groups:
    - Surrogate key  : dbt_utils.generate_surrogate_key on 4 natural key columns
    - Classification : method, type (deposit / withdrawal / refund)
    - Provider info  : provider name, gateway/PSP routing
*/

with source as (
  select * from {{ source('silver', 'ig_payment') }}
),

distinct_methods as (
    select distinct
        payment_method,
        provider_name,
        gateway_name,
        payment_type
    from source
    where payment_method is not null
      and provider_name is not null
      and gateway_name  is not null
      and payment_type  is not null
)

select
    -- -------------------------------------------------------------------------
    -- Surrogate key — dbt_utils handles NULL coalescing and adapter-specific hashing
    -- -------------------------------------------------------------------------
    {{ dbt_utils.generate_surrogate_key(['payment_method', 'provider_name', 'gateway_name', 'payment_type']) }}
                                as sk_payment_method,

    -- -------------------------------------------------------------------------
    -- Classification
    -- -------------------------------------------------------------------------
    payment_method,             -- WALLET / CARD / BANK / BANK_IBAN / VOUCHER …
    payment_type,               -- DEPOSIT / WITHDRAWAL / REFUND

    -- -------------------------------------------------------------------------
    -- Provider / routing info
    -- -------------------------------------------------------------------------
    provider_name,              -- PAYPAL / KLARNA / PAYSAFECARD / NUVEI …
    gateway_name                -- PAYMENT_IQ / SPORTRADAR …

from distinct_methods
