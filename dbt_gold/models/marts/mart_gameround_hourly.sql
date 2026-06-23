{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['sk_date_time', 'sk_game', 'country_code'],
        file_format = 'delta'
    )
}}

/*
  mart_gameround_hourly
  =====================
  Pre-aggregated Gold Mart — grain: one row per (hour × game × country).
  Built on top of fact_gameround. Designed for BI consumption (QlikView QVD).

  Why hourly grain:
    - BI tools load this table fully into QVD once per day (early morning job)
    - Avoids scanning billions of raw fact rows at query time
    - Still granular enough to slice by game, vertical, studio, hour, country
    - sk_date_time joins to dim_date_time for CET/UTC hour, date, month, quarter

  Scheduling:
    - Run daily, early morning, after silver pipeline completes for previous day
    - 2-day lookback window: re-aggregates D-2 and D-1 on every run
    - Handles late-arriving data: if a record for yesterday or day-before-yesterday
      arrives late, this run will pick it up and MERGE the corrected aggregate
    - unique_key = (sk_date_time, sk_game, country_code) → merge updates existing
      rows and inserts new ones safely; safe to rerun multiple times

  KPIs available:
    GGR              = SUM(betAmount_eur) - SUM(winAmount_eur)
    NGR              = GGR - SUM(bonusWinAmount_eur) - SUM(taxAmount_eur)
    Bonus bet ratio  = SUM(bonusBetAmount_eur) / SUM(betAmount_eur)
    Win rate         = SUM(winAmount_eur) / SUM(betAmount_eur)
    RPC              = SUM(betAmount_eur) / COUNT(DISTINCT sk_player)  (revenue per player)
*/

with fact as (
    select * from {{ ref('fact_gameround') }}
    {% if is_incremental() %}
    -- 2-day lookback: re-aggregate D-2 and D-1 on every incremental run
    -- sk_date_time format is YYYYMMDDHH — floor(/ 100) strips the hour → YYYYMMDD integer
    -- Example: today = 2025-06-05
    --   D-2 lookback date = 20250603
    --   D-1 lookback date = 20250604
    --   filter keeps all rows where date part >= 20250603
    where floor(sk_date_time / 100) >= cast(
        date_format(date_sub(current_date(), 2), 'yyyyMMdd')
    as bigint)
    {% endif %}
),

aggregated as (
    select
        -- -----------------------------------------------------------------------
        -- Grain keys — join to dims for full attributes
        -- -----------------------------------------------------------------------
        sk_date_time,                               -- → dim_date_time (hour, date, month, CET/UTC)
        sk_game,                                    -- → dim_game (name, studio, vertical, type)
        country_code,

        -- -----------------------------------------------------------------------
        -- Volume metrics
        -- -----------------------------------------------------------------------
        COUNT(gameRoundId)                          AS rounds,
        COUNT(DISTINCT sk_player)                   AS active_players,
        SUM(betCount)                               AS total_bet_count,
        SUM(winCount)                               AS total_win_count,
        SUM(CASE WHEN promotional = 1 THEN 1 END)   AS promo_rounds,

        -- -----------------------------------------------------------------------
        -- Revenue metrics (EUR normalised)
        -- -----------------------------------------------------------------------
        ROUND(SUM(betAmount_eur), 4)                AS bet_eur,
        ROUND(SUM(winAmount_eur), 4)                AS win_eur,
        ROUND(SUM(bonusBetAmount_eur), 4)           AS bonus_bet_eur,
        ROUND(SUM(bonusWinAmount_eur), 4)           AS bonus_win_eur,
        ROUND(SUM(taxAmount_eur), 4)                AS tax_eur,

        -- -----------------------------------------------------------------------
        -- Derived KPIs (pre-computed for fast BI loading)
        -- -----------------------------------------------------------------------
        ROUND(SUM(betAmount_eur) - SUM(winAmount_eur), 4)
                                                    AS ggr_eur,

        ROUND(SUM(betAmount_eur) - SUM(winAmount_eur)
              - SUM(bonusWinAmount_eur) - SUM(taxAmount_eur), 4)
                                                    AS ngr_eur,

        ROUND(SUM(bonusBetAmount_eur)
              / NULLIF(SUM(betAmount_eur), 0), 6)   AS bonus_bet_ratio,

        ROUND(SUM(winAmount_eur)
              / NULLIF(SUM(betAmount_eur), 0), 6)   AS win_rate

    from fact
    group by
        sk_date_time,
        sk_game,
        country_code
)

select * from aggregated
