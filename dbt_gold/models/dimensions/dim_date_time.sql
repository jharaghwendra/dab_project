/*
  dim_date_time
  =============
  Hourly date spine covering 2025-01-01 → 2026-12-31.
  Each UTC hour is stored twice — once as UTC, once converted to Europe/Vienna (CET/CEST).
  This lets BI reports filter by timezone without doing any conversion at query time.

  Materialization : table  (full rebuild on each dbt run)
  Primary key     : (sk_date_time, timezone)
  Key format      : YYYYMMDDHH in LOCAL time for that timezone row
*/

with date_spine as (
    select explode(
        sequence(date('2025-01-01'), date('2026-12-31'), interval 1 day)
    ) as date_utc
),

hours as (
    select explode(sequence(0, 23, 1)) as hour_utc
),

timezones as (
    select 'UTC'           as timezone
    union all
    select 'Europe/Vienna' as timezone
),

-- One row per UTC timestamp (date × hour)
hourly_utc as (
    select
        d.date_utc,
        h.hour_utc,
        make_timestamp(
            year(d.date_utc), month(d.date_utc), day(d.date_utc),
            h.hour_utc, 0, 0
        ) as ts_utc
    from date_spine d
    cross join hours h
),

-- Cross join with timezones and derive local timestamp per timezone
final as (
    select
        tz.timezone,
        h.ts_utc,
        case
            when tz.timezone = 'UTC' then h.ts_utc
            else from_utc_timestamp(h.ts_utc, 'Europe/Vienna')
        end as ts_local
    from hourly_utc h
    cross join timezones tz
)

select
    -- sk_date_time: YYYYMMDDHH in local time — stable shared key format across Gold models
    cast(
        year(ts_local)    * 1000000
        + month(ts_local) * 10000
        + day(ts_local)   * 100
        + hour(ts_local)
    as int)                                 as sk_date_time,

    cast(ts_local as date)                  as date,
    cast(hour(ts_local)    as tinyint)      as hour,
    cast(day(ts_local)     as tinyint)      as day,
    cast(month(ts_local)   as tinyint)      as month,
    cast(year(ts_local)    as int)          as year,
    cast(quarter(ts_local) as tinyint)      as quarter,
    timezone

from final
