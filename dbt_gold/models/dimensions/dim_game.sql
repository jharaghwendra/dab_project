/*
  dim_game
  ========
  SCD Type 1 — one row per (gameId, country_code), always reflecting the latest state.
  Sources:
    - silver.game      (pk = gameId)          — master game catalog
    - silver.brandgame (pk = brandGameId, fk = gameId) — brand-specific config enrichment

  Join: game LEFT JOIN brandgame on gameId + country_code
  Grain: one row per gameId per country.

  Column groups:
    - Identity    : surrogate key + natural keys
    - Game core   : name, studio, classification (vertical / type / class)
    - Flags       : boolean feature flags (live, enabled, branded, etc.)
    - Performance : rtp, volatility, betting limits
    - Brand layer : brand-specific status and provider info (from brandgame)
    - Timestamps  : release, enable, disable, and record freshness
*/

with game as (
    select * from {{ source('silver', 'game') }}
),

brandgame as (
    select * from {{ source('silver', 'brandgame') }}
),

joined as (
    select
        -- -------------------------------------------------------------------------
        -- Surrogate key
        -- -------------------------------------------------------------------------
        {{ dbt_utils.generate_surrogate_key(['g.gameId', 'g.country_code']) }} as sk_game,

        g.gameId,
        g.country_code,

        -- -------------------------------------------------------------------------
        -- Game core — classification
        -- -------------------------------------------------------------------------
        g.name,
        g.studio,
        g.vertical,
        g.type,
        g.class,
        g.slug,
        g.tableId,                  -- live table ID (populated for live games)

        -- -------------------------------------------------------------------------
        -- Flags — boolean feature flags
        -- -------------------------------------------------------------------------
        g.live,
        g.enabled,
        g.branded,
        g.freeGames,
        g.gamble,
        g.progressiveJackpot,
        g.loginRequired,
        g.bonusGame,
        g.private,
        g.discardable,

        -- -------------------------------------------------------------------------
        -- Performance — RTP, volatility, betting range
        -- -------------------------------------------------------------------------
        g.rtp,
        g.volatility,
        g.minBet,
        g.maxBet,
        g.jackpotId,
        g.allowedWalletType,

        -- -------------------------------------------------------------------------
        -- Brand layer — brand-specific overrides (from brandgame)
        -- NULL when no brandgame row exists for this gameId + country_code
        -- -------------------------------------------------------------------------
        bg.privateStatus,
        bg.gameProviderId,
        bg.gameEnabled             as brandGameEnabled,

        -- -------------------------------------------------------------------------
        -- Timestamps
        -- Note: enabledAt / disabledAt / firstEnabledAt in brandgame contain
        -- millisecond-epoch values misread as seconds in a subset of rows,
        -- producing years like 57044. Guard to NULL to keep the column usable.
        -- -------------------------------------------------------------------------
        g.releasedAt,
        case when year(bg.enabledAt)      > 9999 then null else bg.enabledAt      end as brandEnabledAt,
        case when year(bg.firstEnabledAt) > 9999 then null else bg.firstEnabledAt end as brandFirstEnabledAt,
        case when year(bg.disabledAt)     > 9999 then null else bg.disabledAt     end as brandDisabledAt,
        g.createdAt,
        g.updatedAt                as gameUpdatedAt

    from game g
    left join brandgame bg
        on g.gameId        = bg.gameId
       and g.country_code  = bg.country_code
)

select * from joined
