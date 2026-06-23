/*
    dim_game
    ========
    SCD Type 1 — one row per (game_id, country_code), always reflecting the latest state.
    Sources:
        - silver.ig_game_catalog      (pk = game_id)
        - silver.ig_brand_game_catalog (pk = brand_game_id, fk = game_id)

    Join: ig_game_catalog LEFT JOIN ig_brand_game_catalog on game_id + country_code
    Grain: one row per game_id per country.
*/

with game as (
    select * from {{ source('silver', 'ig_game_catalog') }}
),

brandgame as (
    select * from {{ source('silver', 'ig_brand_game_catalog') }}
),

joined as (
    select
        -- -------------------------------------------------------------------------
        -- Surrogate key
        -- -------------------------------------------------------------------------
        {{ dbt_utils.generate_surrogate_key(['g.game_id', 'g.country_code']) }} as sk_game,

        g.game_id,
        g.country_code,
        g.game_name as name,
        g.game_studio as studio,
        g.game_vertical as vertical,
        g.game_type as type,
        g.game_class as class,
        g.game_slug as slug,
        g.table_id as table_id,
        g.is_live as live,
        g.is_enabled as enabled,
        g.is_branded as branded,
        g.is_free_games as free_games,
        g.is_gamble as gamble,
        g.is_progressive_jackpot as progressive_jackpot,
        g.is_login_required as login_required,
        g.is_bonus_game as bonus_game,
        g.is_private as private,
        g.is_discardable as discardable,
        g.rtp,
        g.volatility,
        g.min_bet,
        g.max_bet,
        g.jackpot_id,
        g.allowed_wallet_type,
        bg.private_status,
        bg.provider_name as game_provider_id,
        bg.is_enabled as brand_game_enabled,
        g.released_at,
        case when year(bg.enabled_at) > 9999 then null else bg.enabled_at end as brand_enabled_at,
        case when year(bg.first_enabled_at) > 9999 then null else bg.first_enabled_at end as brand_first_enabled_at,
        case when year(bg.disabled_at) > 9999 then null else bg.disabled_at end as brand_disabled_at,
        g.ingested_at as created_at,
        g.updated_at as updated_at

    from game g
    left join brandgame bg
        on g.game_id = bg.game_id
           and g.country_code = bg.country_code
)

select * from joined
