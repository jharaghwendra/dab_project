{{
    config(
        unique_key = 'round_id'
    )
}}

/*
    fact_gameround
    ==============
    Grain       : one row per completed game round (round_id)
    Source      : silver.ig_round
    Incremental : merge on round_id — new/updated rounds only each run
    FKs         : sk_date_time → dim_date_time (Vienna hour of created_at)
                                sk_player    → dim_player    (player_id + country_code)
                                sk_game      → dim_game      (game_id + country_code)

  Key KPIs enabled:
    GGR  = SUM(betAmount_eur - winAmount_eur)
    NGR  = SUM(betAmount_eur - winAmount_eur - bonusWinAmount_eur - taxAmount_eur)
    Bonus exposure = SUM(bonusBetAmount_eur)
    Win rate       = SUM(winAmount_eur) / SUM(betAmount_eur)

  Only closed rounds included — open rounds have no final win amount yet.
*/

with source as (
    select * from {{ source('silver', 'ig_round') }}

    {% if is_incremental() %}
        -- on incremental runs: only process rounds updated since last load
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

completed as (
    -- only closed (completed) rounds — open rounds have partial/no winAmount
    select * from source
    where is_closed = 1
),

final as (
    select
        -- -------------------------------------------------------------------------
        -- Natural key
        -- -------------------------------------------------------------------------
        round_id,

        -- -------------------------------------------------------------------------
        -- Foreign keys to dimensions
        -- -------------------------------------------------------------------------

        -- Date/time FK — YYYYMMDDHH in Vienna local time
        cast(
            year(from_utc_timestamp(ingested_at, 'Europe/Vienna'))    * 1000000
            + month(from_utc_timestamp(ingested_at, 'Europe/Vienna')) * 10000
            + day(from_utc_timestamp(ingested_at, 'Europe/Vienna'))   * 100
            + hour(from_utc_timestamp(ingested_at, 'Europe/Vienna'))
        as int)                                     as sk_date_time,

        -- Player FK — matches dim_player.sk_player
        {{ dbt_utils.generate_surrogate_key(['player_id', 'country_code']) }}
                                                    as sk_player,

        -- Game FK — matches dim_game.sk_game
        {{ dbt_utils.generate_surrogate_key(['game_id', 'country_code']) }}
                                                    as sk_game,

        -- -------------------------------------------------------------------------
        -- Degenerate dimensions (attributes useful for slicing, no dim table needed)
        -- -------------------------------------------------------------------------
        country_code,
        currency_code,
        is_promotional,                             -- 1 = bonus/promo round

        -- -------------------------------------------------------------------------
        -- Measures — EUR-normalised (use _eur for cross-currency aggregation)
        -- -------------------------------------------------------------------------
        bet_amount_eur,
        win_amount_eur,
        bonus_bet_amount_eur,
        bonus_win_amount_eur,
        tax_amount_eur,

        -- derived GGR at row level (useful for incremental partial aggregations)
        bet_amount_eur - win_amount_eur             as ggr_eur,

        -- raw bet/win counts within the round
        bet_count,
        win_count,

        -- -------------------------------------------------------------------------
        -- Timestamps — kept for audit and incremental filter
        -- -------------------------------------------------------------------------
        created_at,
        finished_at,
        updated_at

    from completed
)

select * from final
