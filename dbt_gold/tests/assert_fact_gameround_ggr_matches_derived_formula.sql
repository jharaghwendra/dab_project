/*
  Singular test: assert_fact_gameround_ggr_matches_derived_formula
  ----------------------------------------------------------------
  Business rule: ggr_eur is a derived column defined as:
      ggr_eur = betAmount_eur - winAmount_eur

  This test verifies that the materialized value in the table matches
  the formula exactly — catches rounding errors, accidental overwrites,
  or upstream pipeline bugs that produce an inconsistent ggr_eur.

  Tolerance: 0.0001 EUR (to handle floating point precision differences).
  Fails if:  any row has ggr_eur deviating from the formula by > 0.0001.
  Returns:   the offending rows — 0 rows = PASS, any rows = FAIL.
*/

select
    gameRoundId,
    country_code,
    betAmount_eur,
    winAmount_eur,
    ggr_eur                                        as stored_ggr,
    betAmount_eur - winAmount_eur                  as expected_ggr,
    abs(ggr_eur - (betAmount_eur - winAmount_eur)) as deviation
from {{ ref('fact_gameround') }}
where abs(ggr_eur - (betAmount_eur - winAmount_eur)) > 0.0001
