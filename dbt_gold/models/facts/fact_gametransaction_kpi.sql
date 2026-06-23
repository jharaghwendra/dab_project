{{
    config(
        unique_key = 'sk_txn_kpi'
    )
}}

/*
  fact_gametransaction_kpi
  ========================
  Grain   : one row per (sk_date_time × userId × gameId × currencyCode × country_code)
          = hourly, user-level aggregated KPIs from silver.gametransaction
    Note    : brandId excluded from grain — always NULL in source tables

    Business-facing KPI fact table for hourly game transaction analytics.
  BI (QlikView) loads this and does COUNT(DISTINCT userId), SUM(bets_eur) etc. at query time.
  Joins to dim_date_time on sk_date_time to get CET hour/date for reporting.

  Incremental strategy:
    - merge on sk_txn_kpi (surrogate key of all grain columns)
    - 2-day lookback: re-aggregates D-2 and D-1 on every run
    - handles late-arriving transactions and rollback corrections safely

    Business rules implemented in this model:
    1. status = 'SUCCEEDED' only
    2. gameId NOT NULL / NOT blank
    3. TEST users excluded via LEFT ANTI JOIN on silver.ig_player_tag (active TEST tags)
    4. BET/WIN amounts netted against rollbacks per amount type:
         bets = SUM(BET + RESERVED_BET) − SUM(ROLLBACK_BET + ROLLBACK_RESERVED_BET + ROLLBACK_ROUND)
         wins = SUM(WIN + PAYOUT + CASHOUT) − SUM(ROLLBACK_WIN + ROLLBACK_CASHOUT)
       Applied separately for: total (_eur), real money (_real_eur), bonus (_bonus_eur)
    5. Promotional wins NOT rollback-adjusted (by design for current KPI contract)
    6. is_casino_player / is_sports_player flags from active CASINO_PLAYER / SPORTS_PLAYER tags
*/

-- ---------------------------------------------------------------------------
-- dim_game: pull stable game attributes for denormalization into fact
-- QlikView loads single QVD with no joins needed — vertical/studio/provider
-- are stable (rarely change) so SCD risk is negligible
-- ---------------------------------------------------------------------------
with dim_game as (
    select
        gameId,
        country_code,
        vertical,       -- Casino / Sports / Live etc.
        studio,         -- game developer/studio name
        type            -- game type (SLOT, TABLE, LIVE_CASINO etc.)
    from {{ ref('dim_game') }}
),

-- ---------------------------------------------------------------------------
-- Test users: excluded from all KPIs
-- ---------------------------------------------------------------------------
test_users as (
    select distinct
        targetId    as userId,
        country_code
    from {{ source('silver', 'ig_player_tag') }}
    where targetType  = 'User'
      and tagCategory = 'TEST'
      and active      = true
),

-- ---------------------------------------------------------------------------
-- Player vertical flags: Casino vs Sports (a user can be both simultaneously)
-- ---------------------------------------------------------------------------
player_verticals as (
    select
        targetId     as userId,
        country_code,
        max(case when tagCategory = 'CASINO_PLAYER' and active = true then 1 else 0 end)  as is_casino_player,
        max(case when tagCategory = 'SPORTS_PLAYER' and active = true then 1 else 0 end)  as is_sports_player
    from {{ source('silver', 'ig_player_tag') }}
    where targetType = 'User'
      and tagCategory in ('CASINO_PLAYER', 'SPORTS_PLAYER')
    group by targetId, country_code
),

-- ---------------------------------------------------------------------------
-- Source: filter to SUCCEEDED transactions with valid gameId
-- Pre-compute sk_date_time (Vienna YYYYMMDDHH) and clean gameId here
-- so GROUP BY downstream is simple column references
-- ---------------------------------------------------------------------------
source as (
    select
        * except (gameId),
        trim(gameId)  as gameId,      -- strip whitespace before aggregation and joins
        cast(
            year(from_utc_timestamp(createdAt, 'Europe/Vienna'))    * 1000000
            + month(from_utc_timestamp(createdAt, 'Europe/Vienna')) * 10000
            + day(from_utc_timestamp(createdAt, 'Europe/Vienna'))   * 100
            + hour(from_utc_timestamp(createdAt, 'Europe/Vienna'))
        as int)       as sk_date_time  -- YYYYMMDDHH in Vienna time → joins to dim_date_time
    from {{ source('silver', 'ig_transaction') }}
    where status  = 'SUCCEEDED'
      and gameId  is not null
      and trim(gameId) <> ''
    {% if is_incremental() %}
    -- 2-day lookback: go back 2 days in UTC — safely covers D-2 Vienna
    -- (Vienna is UTC+1/+2 so UTC D-2 00:00 is before Vienna D-2 00:00)
    and createdAt >= cast(date_format(date_sub(current_date(), 2), 'yyyy-MM-dd') as timestamp)
    {% endif %}
),

-- ---------------------------------------------------------------------------
-- Exclude test users via LEFT ANTI JOIN
-- ---------------------------------------------------------------------------
real_transactions as (
    select gt.*
    from source gt
    left anti join test_users tu
        on  tu.userId       = gt.userId
        and tu.country_code = gt.country_code
),

-- ---------------------------------------------------------------------------
-- Aggregate to grain: sk_date_time × userId × gameId × currency × brand × country
-- ---------------------------------------------------------------------------
aggregated as (
    select
        sk_date_time,
        userId,
        gameId,
        currencyCode,
        country_code,

        -- ---------------------------------------------------------------
        -- BETS — net of rollbacks (EUR normalised)
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN gameTransactionType IN ('BET','RESERVED_BET')
                          THEN coalesce(amount_eur, 0) ELSE 0 END)
            - SUM(CASE WHEN gameTransactionType IN ('ROLLBACK_BET','ROLLBACK_RESERVED_BET','ROLLBACK_ROUND')
                          THEN coalesce(amount_eur, 0) ELSE 0 END)
        , 4)                                                            as bets_eur,

        -- ---------------------------------------------------------------
        -- WINS — net of rollbacks (EUR normalised)
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN gameTransactionType IN ('WIN','PAYOUT','CASHOUT')
                          THEN coalesce(amount_eur, 0) ELSE 0 END)
            - SUM(CASE WHEN gameTransactionType IN ('ROLLBACK_WIN','ROLLBACK_CASHOUT')
                          THEN coalesce(amount_eur, 0) ELSE 0 END)
        , 4)                                                            as wins_eur,

        -- ---------------------------------------------------------------
        -- BETS REAL — real money only, net of rollbacks
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN gameTransactionType IN ('BET','RESERVED_BET')
                          THEN coalesce(amountReal_eur, 0) ELSE 0 END)
            - SUM(CASE WHEN gameTransactionType IN ('ROLLBACK_BET','ROLLBACK_RESERVED_BET','ROLLBACK_ROUND')
                          THEN coalesce(amountReal_eur, 0) ELSE 0 END)
        , 4)                                                            as bets_real_eur,

        -- ---------------------------------------------------------------
        -- WINS REAL — real money only, net of rollbacks
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN gameTransactionType IN ('WIN','PAYOUT','CASHOUT')
                          THEN coalesce(amountReal_eur, 0) ELSE 0 END)
            - SUM(CASE WHEN gameTransactionType IN ('ROLLBACK_WIN','ROLLBACK_CASHOUT')
                          THEN coalesce(amountReal_eur, 0) ELSE 0 END)
        , 4)                                                            as wins_real_eur,

        -- ---------------------------------------------------------------
        -- BETS BONUS — bonus money only, net of rollbacks
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN gameTransactionType IN ('BET','RESERVED_BET')
                          THEN coalesce(amountBonus_eur, 0) ELSE 0 END)
            - SUM(CASE WHEN gameTransactionType IN ('ROLLBACK_BET','ROLLBACK_RESERVED_BET','ROLLBACK_ROUND')
                          THEN coalesce(amountBonus_eur, 0) ELSE 0 END)
        , 4)                                                            as bets_bonus_eur,

        -- ---------------------------------------------------------------
        -- WINS BONUS — bonus money only, net of rollbacks
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN gameTransactionType IN ('WIN','PAYOUT','CASHOUT')
                          THEN coalesce(amountBonus_eur, 0) ELSE 0 END)
            - SUM(CASE WHEN gameTransactionType IN ('ROLLBACK_WIN','ROLLBACK_CASHOUT')
                          THEN coalesce(amountBonus_eur, 0) ELSE 0 END)
        , 4)                                                            as wins_bonus_eur,

        -- ---------------------------------------------------------------
        -- PROMOTIONAL WINS — NOT rollback-adjusted (by design for KPI consistency)
        -- ---------------------------------------------------------------
        ROUND(
            SUM(CASE WHEN promotional = 1
                      AND gameTransactionType IN ('WIN','PAYOUT','CASHOUT')
                     THEN coalesce(amountBonus_eur, 0) ELSE 0 END)
        , 4)                                                            as promo_wins_bonus_eur,

        ROUND(
            SUM(CASE WHEN promotional = 1
                      AND gameTransactionType IN ('WIN','PAYOUT','CASHOUT')
                     THEN coalesce(amountReal_eur, 0) + coalesce(amountBonus_eur, 0)
                     ELSE 0 END)
        , 4)                                                            as total_promo_wins_eur,

        -- ---------------------------------------------------------------
        -- Audit
        -- ---------------------------------------------------------------
        COUNT(gameTransactionId)                                        as src_row_count

    from real_transactions
    group by
        sk_date_time,
        userId,
        gameId,
        currencyCode,
        country_code
),

-- ---------------------------------------------------------------------------
-- Final: add surrogate key + derived GGR columns + vertical flags
-- ---------------------------------------------------------------------------
final as (
    select
        -- Merge key (surrogate of all grain columns)
        {{ dbt_utils.generate_surrogate_key([
            'a.sk_date_time', 'a.userId', 'a.gameId',
            'a.currencyCode', 'a.country_code'
        ]) }}                                                           as sk_txn_kpi,

        -- Grain
        a.sk_date_time,      -- → dim_date_time (hour, date, CET/UTC)
        a.userId,
        a.gameId,
        a.currencyCode,
        a.country_code,

        -- Game attributes (denormalized from dim_game — stable, avoids join in Qlik QVD)
        -- NULL = game unavailable in dimension at load time (orphaned transactions) → labelled UNKNOWN
        coalesce(dg.vertical, 'UNKNOWN')    as vertical,
        coalesce(dg.studio,   'UNKNOWN')    as studio,
        coalesce(dg.type,     'UNKNOWN')    as game_type,  -- SLOT / TABLE / LIVE_CASINO etc.

        -- KPI measures (EUR)
        a.bets_eur,
        a.wins_eur,
        ROUND(a.bets_eur - a.wins_eur, 4)                              as ggr_eur,

        a.bets_real_eur,
        a.wins_real_eur,
        ROUND(a.bets_real_eur - a.wins_real_eur, 4)                    as ggr_real_eur,

        a.bets_bonus_eur,
        a.wins_bonus_eur,
        ROUND(a.bets_bonus_eur - a.wins_bonus_eur, 4)                  as ggr_bonus_eur,

        a.promo_wins_bonus_eur,
        a.total_promo_wins_eur,

        -- Player vertical flags (0 if user not in tag table yet)
        coalesce(pv.is_casino_player, 0)                               as is_casino_player,
        coalesce(pv.is_sports_player, 0)                               as is_sports_player,

        -- Audit
        a.src_row_count,
        current_timestamp()                                             as last_update

    from aggregated a
    left join player_verticals pv
        on  pv.userId       = a.userId
        and pv.country_code = a.country_code
    left join dim_game dg
        on  dg.gameId       = a.gameId
        and dg.country_code = a.country_code
)

select * from final
