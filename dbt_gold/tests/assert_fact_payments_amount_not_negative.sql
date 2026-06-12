/*
  Singular test: assert_fact_payments_amount_not_negative
  --------------------------------------------------------
  Business rule: all SUCCEEDED payments must have a positive amount_eur.
  A negative amount_eur would indicate a data quality issue upstream —
  refunds are modelled with payment_type = 'REFUND' (not negative amounts).

  Fails if: any row has amount_eur <= 0.
  Returns:  the offending rows — 0 rows = PASS, any rows = FAIL.
*/

select
    sk_payment,
    paymentId,
    country_code,
    payment_type,
    amount_eur
from {{ ref('fact_payments') }}
where amount_eur <= 0
