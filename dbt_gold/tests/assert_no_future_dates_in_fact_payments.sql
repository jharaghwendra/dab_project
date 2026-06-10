/*
  Singular test: assert_no_future_dates_in_fact_payments
  -------------------------------------------------------
  Business rule: a payment's sk_date_time must never represent a future hour.
  sk_date_time is an INT in the format YYYYMMDDHH (e.g. 2026060911 = 9am on 9 Jun 2026).
  We extract the date portion (divide by 100, floor) and compare to today.

  Fails if: any row has an sk_date_time whose date portion is in the future.
  Returns:  the offending rows — 0 rows = PASS, any rows = FAIL.
*/

select
    sk_payment,
    sk_date_time,
    createdAt
from {{ ref('fact_payments') }}
where to_date(cast(floor(sk_date_time / 100) as string), 'yyyyMMdd') > current_date()
