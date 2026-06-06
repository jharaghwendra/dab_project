{{
    config(
        unique_key = 'gameRoundId'
    )
}}

/*
  fact_game_rounds
  ================
  Grain       : one row per completed game round (gameRoundId)
  Source      : silver.gameround
  Incremental : merge on gameRoundId — new/updated rounds only each run
  FKs         : sk_date_time → dim_date_time (Vienna hour of createdAt)
                sk_player    → dim_player    (userId + country_code)
                sk_game      → dim_game      (gameId + country_code)

  Key KPIs enabled:
    GGR  = SUM(betAmount_eur - winAmount_eur)
    NGR  = SUM(betAmount_eur - winAmount_eur - bonusWinAmount_eur - taxAmount_eur)
    Bonus exposure = SUM(bonusBetAmount_eur)
    Win rate       = SUM(winAmount_eur) / SUM(betAmount_eur)

  Only closed rounds included — open rounds have no final win amount yet.
*/

with source as (
    select * from {{ source('silver', 'gameround') }}

    {% if is_incremental() %}
        -- on incremental runs: only process rounds updated since last load
        where updatedAt > (select max(updatedAt) from {{ this }})
    {% endif %}
),

completed as (
    -- only closed (completed) rounds — open rounds have partial/no winAmount
    select * from source
    where closed = 1
),

final as (
    select
        -- -------------------------------------------------------------------------
        -- Natural key
        -- -------------------------------------------------------------------------
        gameRoundId,

        -- -------------------------------------------------------------------------
        -- Foreign keys to dimensions
        -- -------------------------------------------------------------------------

        -- Date/time FK — YYYYMMDDHH in Vienna local time
        cast(
            year(from_utc_timestamp(createdAt, 'Europe/Vienna'))    * 1000000
            + month(from_utc_timestamp(createdAt, 'Europe/Vienna')) * 10000
            + day(from_utc_timestamp(createdAt, 'Europe/Vienna'))   * 100
            + hour(from_utc_timestamp(createdAt, 'Europe/Vienna'))
        as int)                                     as sk_date_time,

        -- Player FK — matches dim_player.sk_player
        {{ dbt_utils.generate_surrogate_key(['userId', 'country_code']) }}
                                                    as sk_player,

        -- Game FK — matches dim_game.sk_game
        {{ dbt_utils.generate_surrogate_key(['gameId', 'country_code']) }}
                                                    as sk_game,

        -- -------------------------------------------------------------------------
        -- Degenerate dimensions (attributes useful for slicing, no dim table needed)
        -- -------------------------------------------------------------------------
        country_code,
        currencyCode,
        promotional,                                -- 1 = bonus/promo round

        -- -------------------------------------------------------------------------
        -- Measures — EUR-normalised (use _eur for cross-currency aggregation)
        -- -------------------------------------------------------------------------
        betAmount_eur,
        winAmount_eur,
        bonusBetAmount_eur,
        bonusWinAmount_eur,
        taxAmount_eur,

        -- derived GGR at row level (useful for incremental partial aggregations)
        betAmount_eur - winAmount_eur               as ggr_eur,

        -- raw bet/win counts within the round
        betCount,
        winCount,

        -- -------------------------------------------------------------------------
        -- Timestamps — kept for audit and incremental filter
        -- -------------------------------------------------------------------------
        createdAt,
        finishedAt,
        updatedAt

    from completed
)

select * from final
