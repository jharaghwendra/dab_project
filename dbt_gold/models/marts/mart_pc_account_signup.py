# =============================================================================
# mart_pc_account_signup — dbt Python model (Phase 2)
# =============================================================================
# Replaces Talend job: pc_account_signup_incremental_load
#
# PURPOSE:
#   Stages one row per player signup event, with the full PowerComply
#   JSON payload pre-built and ready for the dispatcher notebook to POST.
#
# TALEND JOB REPLACED:
#   Fetch_Account_Signup_Data (MySQL SELECT with last_run_timestamp window)
#   Build_JSON_Payload         (TJava: manual StringBuilder — fragile, no type safety)
#   RestClient_To_Post_Data    (row-by-row POST to PowerComply endpoint)
#
# THIS MODEL handles concerns 1+2 (data + transformation) only.
# The dispatcher notebook handles concern 3 (HTTP POST).
#
# KEY IMPROVEMENTS OVER TALEND:
#   1. tagCategories JSON: Python json.dumps() produces "active":true (boolean)
#      Talend GROUP_CONCAT produced "active":1 (MySQL integer) — PowerComply
#      REST API expects boolean. This was a silent data quality bug in Talend.
#   2. Incremental watermark: dbt is_incremental tracks max(updatedAt) seen,
#      no separate job_control table needed.
#   3. dbt tests validate payload shape before dispatcher runs.
#   4. Full lineage: dim_player + dim_tag → mart_pc_account_signup visible in dbt docs.
#
# INPUTS:
#   dbt.ref("dim_player") — SCD2 snapshot, current rows only
#                           (WHERE dbt_valid_to = '9999-12-31')
#   dbt.ref("dim_tag")    — SCD2 snapshot, current active tags per player
#                           (WHERE dbt_valid_to = '9999-12-31' AND active = true
#                            AND targetType = 'User')
#
# OUTPUT: igaming_dev.gold.mart_pc_account_signup
#   One row per player (userId + country_code).
#   Key columns:
#     online_customer_id  — userId cast to string (PC endpoint field name)
#     payload_json        — full serialized JSON string ready to POST
#     dispatched_at       — NULL until dispatcher marks it sent
#     dispatch_error      — NULL until dispatcher records a failure
#     source_updated_at   — updatedAt from dim_player (watermark column)
#
# INCREMENTAL STRATEGY — HOW IT WORKS (runs every 10 minutes)
# =============================================================================
#
# MERGE KEY: (online_customer_id, country_code)
# WATERMARK: source_updated_at column = dim_player.updatedAt at time of staging
#
# ─────────────────────────────────────────────────────────────────────────────
# RUN 1 — First ever run (table does not exist yet)
# ─────────────────────────────────────────────────────────────────────────────
#   dbt.is_incremental = False  →  no watermark filter applied
#   → ALL current players from dim_player are loaded (~180,000 rows)
#   → ALL rows written with dispatched_at = NULL
#   → dbt materializes as: CREATE TABLE ... AS SELECT ...
#
#   mart_pc_account_signup after Run 1:
#   +---------+------------+---------------------+-------------+
#   | userId  |country_code| source_updated_at   |dispatched_at|
#   +---------+------------+---------------------+-------------+
#   | 222217  | DE         | 2024-11-15 09:12:00 | null        | ← not yet POSTed
#   | 23444   | DE         | 2024-11-28 07:45:00 | null        |
#   | 82927   | AT         | 2024-08-05 11:00:00 | null        |
#   | ...     | ...        | ...                 | null        | (180k rows)
#   +---------+------------+---------------------+-------------+
#   max(source_updated_at) = '2024-12-03 08:45:00' (most recent player update)
#
# ─────────────────────────────────────────────────────────────────────────────
# Between Run 1 and Run 2 (the 10-minute window)
# ─────────────────────────────────────────────────────────────────────────────
#   nb_pc_dispatcher runs, POSTs rows WHERE dispatched_at IS NULL
#   → updates dispatched_at = current_timestamp() on success
#   → updates dispatch_error on failure
#
#   Meanwhile in silver layer:
#   → player 222217 updated their email at 2024-12-03 09:10:00 (updatedAt changes)
#   → player 82927 got a new VIP tag at 2024-12-03 09:05:00 (dim_tag dbt_updated_at changes,
#     but dim_player updatedAt does NOT change — tag change is invisible to profile watermark)
#
# ─────────────────────────────────────────────────────────────────────────────
# RUN 2 — Incremental run (10 minutes later)
# ─────────────────────────────────────────────────────────────────────────────
#   dbt.is_incremental = True
#   max_seen = SELECT MAX(source_updated_at) FROM mart_pc_account_signup
#            = '2024-12-03 08:45:00'
#
#   Filter 1 — profile changes:
#     dim_player WHERE updatedAt > '2024-12-03 08:45:00'
#     → catches player 222217 (email changed at 09:10:00) ✓
#     → misses player 82927  (only tags changed, updatedAt untouched) ✗
#
#   Filter 2 — tag-only changes:
#     dim_tag WHERE dbt_updated_at > '2024-12-03 08:45:00'
#     → catches player 82927 (new VIP tag at 09:05:00) ✓
#     → joined back to dim_player to get full profile row
#
#   UNION both → 2 rows in dim_player delta
#
#   MERGE INTO mart_pc_account_signup:
#   ┌──────────────────────────────────────────────────────────────┐
#   │ WHEN MATCHED (online_customer_id + country_code exists)      │
#   │   → UPDATE all columns including:                           │
#   │       payload_json    = freshly rebuilt JSON                 │
#   │       source_updated_at = new updatedAt                      │
#   │       dispatched_at   = NULL  ← RESET! dispatcher re-sends  │
#   │       dispatch_error  = NULL  ← cleared                     │
#   │ WHEN NOT MATCHED (new player since last run)                 │
#   │   → INSERT new row with dispatched_at = NULL                 │
#   └──────────────────────────────────────────────────────────────┘
#
#   After Run 2 merge:
#   +---------+------------+---------------------+-----------------------------+
#   | userId  |country_code| source_updated_at   | dispatched_at               |
#   +---------+------------+---------------------+-----------------------------+
#   | 222217  | DE         | 2024-12-03 09:10:00 | null  ← RESET, re-dispatch  |
#   | 23444   | DE         | 2024-11-28 07:45:00 | 2024-12-03 08:50:00 (sent)  | (untouched)
#   | 82927   | AT         | 2024-08-05 11:00:00 | null  ← RESET, re-dispatch  |
#   | ...     | ...        | ...                 | ...                         |
#   +---------+------------+---------------------+-----------------------------+
#
# KEY DESIGN DECISIONS:
#   1. dispatched_at reset on merge update is INTENTIONAL — ensures re-dispatch
#      when player profile or tags change. PowerComply signup is idempotent.
#   2. Two watermarks (profile + tag) ensure no change slips through, even when
#      dim_player.updatedAt is not touched by tag events.
#   3. Dispatcher and dbt model are decoupled — they can run at different cadences.
#      dbt every 10 min, dispatcher can run every 5 min or on-demand.
# =============================================================================
#
# INCREMENTAL STRATEGY (short summary):
#   merge on (online_customer_id, country_code)
#   Lookback: players whose updatedAt > max(source_updated_at) in this table
#   OR whose tag changed (tag_updated_at > max(source_updated_at))
#   On re-run: dispatched_at is reset to NULL → dispatcher will re-send
#   (safe: PowerComply signup is idempotent on onlineCustomerId)
#
# DISPATCHER CONTRACT (nb_pc_dispatcher.py reads this):
#   SELECT * FROM mart_pc_account_signup WHERE dispatched_at IS NULL
#   POST payload_json to PowerComply endpoint row by row
#   On 200: UPDATE dispatched_at = current_timestamp()
#   On error: UPDATE dispatch_error = response body
# =============================================================================

import json
from datetime import datetime, timezone

from pyspark.sql.functions import coalesce, col, collect_set, lit, max, struct, to_json, udf, when
from pyspark.sql.types import StringType


# -----------------------------------------------------------------------------
# Helper functions — replicate the Talend TJava Build_JSON_Payload logic
# These are defined at module level, outside model(), as pure Python functions.
# No Jinja allowed in .py model files.
# -----------------------------------------------------------------------------


def clean_str(val):
    """
    Replicate Talend: replaceAll("[\\r\\n\\t\\u2028\\u2029]", "").trim()
    Then treat empty string and literal "null" as None.
    Returns cleaned string or None.
    """
    if val is None:
        return None
    cleaned = (
        str(val)
        .replace("\r", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace("\u2028", "")
        .replace("\u2029", "")
        .strip()
    )
    if cleaned == "" or cleaned.lower() == "null":
        return None
    return cleaned


def parse_postcode(val):
    """
    Replicate Talend: Integer.parseInt(trimmed) only if numeric digits.
    Returns int or None.
    """
    if val is None:
        return None
    digits = "".join(c for c in str(val).strip() if c.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except (ValueError, OverflowError):
        return None


def parse_mobile(val):
    """
    Replicate Talend: Long.parseLong(digitsOnly) — digits only, no formatting.
    Returns int or None.
    """
    if val is None:
        return None
    digits = "".join(c for c in str(val) if c.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except (ValueError, OverflowError):
        return None


def build_payload(row):
    """
    Build the full PowerComply JSON payload for one player signup event.
    Replicates the Talend TJava Build_JSON_Payload step — cleanly and correctly.

    KEY FIX vs Talend:
      Talend GROUP_CONCAT:  "active":1   (MySQL TINYINT — wrong type)
      Python json.dumps():  "active":true (Python bool — correct for REST API)

    The 'tags' field is the tagCategories JSON built from dim_tag aggregation.
    It arrives as a Python list of dicts (already parsed from JSON string),
    so json.dumps() serializes booleans correctly.
    """
    sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # birthDate: DateType in silver → format as yyyy-MM-dd string
    birth_date_str = None
    if row.birthDate is not None:
        try:
            birth_date_str = row.birthDate.strftime("%Y-%m-%d")
        except AttributeError:
            birth_date_str = str(row.birthDate)[:10]  # fallback for string dates

    # createdAt: TimestampType → ISO-8601 format
    created_on_str = None
    if row.createdAt is not None:
        try:
            created_on_str = row.createdAt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except AttributeError:
            created_on_str = str(row.createdAt)

    # tagCategories: aggregated list of dicts from dim_tag join
    # Already a Python list — json.dumps() handles bool serialization correctly
    tags_val = None
    if row.tagCategories is not None:
        try:
            tags_val = json.loads(row.tagCategories)
        except (json.JSONDecodeError, TypeError):
            tags_val = None

    payload = {
        "collection": "Account",
        "onlineCustomerId": str(row.userId),
        "account": {
            "eventType": "signup",
            "birthdate": birth_date_str,
            "city": clean_str(row.city),
            "country": row.countryCode,
            "isTestUser": row.isTestUser,  # 'Y' or 'N'
            "email": clean_str(row.email),
            "firstName": clean_str(row.firstName),
            "gender": clean_str(row.gender),
            "lastName": clean_str(row.lastName),
            "postcode": parse_postcode(row.postcode),
            "street": clean_str(row.address1),
            "birthPlace": clean_str(row.birthCity),
            "birthCountry": None,  # not in TMA source — matches Talend NULL
            "birthName": clean_str(row.maidenName),
            "nationality": clean_str(row.nationality),
            "mobilenumber": parse_mobile(row.phone),
            "jobposition": None,  # not in TMA source
            "sourceFunds": None,  # not in TMA source
            "countryIncome": None,  # not in TMA source
            "expectedMonthlyTurnover": None,  # not in TMA source
            "title": None,  # not in TMA source
            "iban": None,  # not in TMA source
            "autoPayout": None,  # not in TMA source
            "autoPayoutLimit": None,  # not in TMA source
            "occupation": None,  # not present in dim_player
            # KEY: Python bool serialization — "active":true not "active":1
            "tags": tags_val,
            "createdOn": created_on_str,
        },
        "sentAt": sent_at,
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


# Register as Spark UDF — applied per row on the cluster
build_payload_udf = udf(build_payload, StringType())


# =============================================================================
# dbt model entry point
# =============================================================================


def model(dbt, session):
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["online_customer_id", "country_code"],
        file_format="delta",
        submission_method="serverless_cluster",
    )

    # -------------------------------------------------------------------------
    # Step 1: load dim_player — current rows only (SCD2 open rows)
    # dbt.ref() returns a PySpark DataFrame — stays distributed
    # -------------------------------------------------------------------------
    dim_player = dbt.ref("dim_player").filter(col("dbt_valid_to") == lit("9999-12-31").cast("timestamp"))

    # dim_player.show(3, truncate=True) — key cols (130+ actual cols):
    # +-----------------------+--------+------------+----------+---------------------+
    # | userId                | city   | countryCode| birthDate| updatedAt           |
    # +-----------------------+--------+------------+----------+---------------------+
    # | 222217                | Berlin | DE         |1985-03-22|2024-11-15 09:12:00  |
    # | 5UmnRwIxZyJLAsasanu8  | Wien   | AT         |1991-07-04|2024-12-01 14:30:00  |
    # | 77341                 | Hamburg| DE         |1978-09-11|2024-12-03 08:45:00  |
    # +-----------------------+--------+------------+----------+---------------------+
    # ~180,000 rows — one active SCD2 row per player per country shard

    # -------------------------------------------------------------------------
    # Step 2: load dim_tag — current active tags per player
    # Filter: User tags only, active only, open SCD2 rows only
    # -------------------------------------------------------------------------
    dim_tag = dbt.ref("dim_tag").filter(
        (col("dbt_valid_to") == lit("9999-12-31").cast("timestamp")) & (col("targetType") == "User") & col("active")
    )

    # dim_tag.show(4, truncate=False) — (active=true + targetType='User' + dbt_valid_to='9999-12-31'):
    # +----------------------+------------+-------------+------+---------------------+
    # | targetId             |country_code| tagCategory |active| dbt_updated_at      |
    # +----------------------+------------+-------------+------+---------------------+
    # | 222217               | DE         | TEST        | true |2024-11-20 10:00:00  |
    # | 222217               | DE         | VIP         | true |2024-11-20 10:00:00  |
    # | 23444                | DE         | HIGH_RISK   | true |2024-12-01 08:00:00  |
    # | 82927                | AT         | VIP         | true |2024-11-30 15:22:00  |
    # +----------------------+------------+-------------+------+---------------------+
    # ~12,000 rows — note: user 222217 has 2 rows (TEST + VIP); one row per active tag

    # -------------------------------------------------------------------------
    # Step 3: incremental filter — only players updated since last run
    # Mirrors the Talend WHERE last_update > lastRunTimestamp logic.
    # Also picks up players whose tags changed (tag_updated_at watermark).
    # -------------------------------------------------------------------------
    if dbt.is_incremental:
        # Read max watermark from the existing mart table
        max_seen = session.sql(f"SELECT COALESCE(MAX(source_updated_at), '1970-01-01') FROM {dbt.this}").collect()[0][0]

        # Players whose profile changed
        dim_player = dim_player.filter(col("updatedAt") > lit(max_seen))

        # Also include players whose tags changed since last run
        # (tag change doesn't touch updatedAt on dim_player)
        changed_tag_users = (
            dim_tag.filter(col("dbt_updated_at") > lit(max_seen))
            .select(col("targetId").alias("userId"), col("country_code"))
            .distinct()
        )

        # changed_tag_users.show(3) — players whose tags changed since last run:
        # +----------------------+------------+
        # | userId               |country_code|
        # +----------------------+------------+
        # | 222217               | DE         |
        # | 82927                | AT         |
        # +----------------------+------------+
        # Small delta set — only players with tag activity after max(source_updated_at)

        # Union: profile changes + tag changes
        dim_player = dim_player.union(
            dbt.ref("dim_player")
            .filter(col("dbt_valid_to") == lit("9999-12-31").cast("timestamp"))
            .join(changed_tag_users, on=["userId", "country_code"], how="inner")
        ).distinct()

        # dim_player after union.show(3) — profile changes + tag-only changes merged:
        # +----------------------+------------+---------------------+
        # | userId               |country_code| updatedAt           |
        # +----------------------+------------+---------------------+
        # | 222217               | DE         |2024-11-15 09:12:00  |  <- profile changed
        # | 5UmnRwIxZyJLAsasanu8 | AT         |2024-12-01 14:30:00  |  <- profile changed
        # | 82927                | AT         |2024-08-05 11:00:00  |  <- tag-only change
        # +----------------------+------------+---------------------+
        # Subset of full player table — only the delta for this incremental run

    # -------------------------------------------------------------------------
    # Step 4: aggregate tags per player → tagCategories JSON array
    # Replicates the Talend subquery:
    #   GROUP_CONCAT(DISTINCT CONCAT('{"tagCategory":"', t.tagCategory, '","active":', t.active, '}'))
    # But correctly: Python/Spark produces "active":true not "active":1
    #
    # Result: one row per (userId, country_code) with:
    #   tagCategories = '[{"tagCategory":"TEST","active":true},{"tagCategory":"VIP","active":true}]'
    #   isTestUser    = 'Y' if any active TEST tag, else 'N'
    # -------------------------------------------------------------------------
    tag_agg = dim_tag.groupBy("targetId", "country_code").agg(
        # Build struct array then serialize to JSON
        to_json(
            collect_set(
                struct(
                    col("tagCategory"),
                    col("active"),  # BooleanType → serializes as true/false
                )
            )
        ).alias("tagCategories"),
        # isTestUser: 'Y' if any active TEST tag exists for this player
        max(when(col("tagCategory") == "TEST", lit("Y")).otherwise(lit("N"))).alias("isTestUser"),
        # Track max tag updatedAt for watermark
        max("dbt_updated_at").alias("tag_updated_at"),
    )

    # tag_agg.show(3, truncate=False) — one row per player, tags collapsed to JSON:
    # +--------+------------+--------------------------------------------------------------------------+----------+---------------------+
    # |targetId|country_code|tagCategories                                                             |isTestUser|tag_updated_at       |
    # +--------+------------+--------------------------------------------------------------------------+----------+---------------------+
    # |222217  |DE          |[{"tagCategory":"TEST","active":true},{"tagCategory":"VIP","active":true}] |Y         |2024-11-20 10:00:00  |
    # |23444   |DE          |[{"tagCategory":"HIGH_RISK","active":true}]                                |N         |2024-12-01 08:00:00  |
    # |82927   |AT          |[{"tagCategory":"VIP","active":true}]                                     |N         |2024-11-30 15:22:00  |
    # +--------+------------+--------------------------------------------------------------------------+----------+---------------------+
    # ~8,000 rows | KEY FIX: "active":true (bool) — Talend GROUP_CONCAT produced "active":1 (int)

    # -------------------------------------------------------------------------
    # Step 5: join player profile + tag aggregation
    # LEFT JOIN — players without any tags get isTestUser='N', tagCategories=null
    # -------------------------------------------------------------------------
    joined = dim_player.join(
        tag_agg,
        on=[
            dim_player["userId"] == tag_agg["targetId"],
            dim_player["country_code"] == tag_agg["country_code"],
        ],
        how="left",
    ).select(
        # Natural key (PC endpoint field)
        dim_player["userId"].alias("online_customer_id"),
        dim_player["country_code"],
        # All player fields needed by build_payload UDF
        dim_player["userId"],
        dim_player["countryCode"],
        dim_player["birthDate"],
        dim_player["city"],
        dim_player["email"],
        dim_player["firstName"],
        dim_player["gender"],
        dim_player["lastName"],
        dim_player["postcode"],
        dim_player["address1"],
        dim_player["birthCity"],
        dim_player["maidenName"],
        dim_player["nationality"],
        dim_player["phone"],
        dim_player["createdAt"],
        # Tag aggregation fields
        coalesce(tag_agg["isTestUser"], lit("N")).alias("isTestUser"),
        tag_agg["tagCategories"],
        tag_agg["tag_updated_at"],
        # Watermark — used by next incremental run
        dim_player["updatedAt"].alias("source_updated_at"),
        # ── HOW dispatched_at = NULL on MERGE MATCHED works ──────────────────
        # dbt does NOT have special logic for this. The mechanism is simpler:
        #
        # dbt's merge strategy does:
        #   WHEN MATCHED  → UPDATE SET every_column = value_from_incoming_dataframe
        #   WHEN NOT MATCHED → INSERT   every_column = value_from_incoming_dataframe
        #
        # Both branches copy ALL columns from the dataframe we return here.
        # So whatever value we put in this dataframe IS what gets written.
        #
        # We put lit(None) here → BOTH MATCHED and NOT MATCHED get NULL.
        # That is the entire "reset" mechanism — it's in this one line below.
        # There is no separate dbt config, no merge_update_columns override,
        # no post-hook. Just: incoming row has NULL → merge writes NULL.
        #
        # If we wanted to PRESERVE the existing dispatched_at on match (i.e.
        # only reset for truly new/changed rows), we would need to add a
        # post-hook or use merge_update_columns to exclude dispatched_at.
        # We intentionally do NOT do that — resetting on every profile/tag
        # change is the correct behaviour (ensures re-dispatch with fresh payload).
        # ─────────────────────────────────────────────────────────────────────
        lit(None).cast("timestamp").alias("dispatched_at"),
        lit(None).cast("string").alias("dispatch_error"),
    )

    # joined.show(3, truncate=True) — all players LEFT JOINed with tag summary:
    # +----------------------+------------+----------+---------------------+---------------------+-------------+
    # |online_customer_id    |country_code|isTestUser|tagCategories        |source_updated_at    |dispatched_at|
    # +----------------------+------------+----------+---------------------+---------------------+-------------+
    # |222217                |DE          |Y         |[{"tagCategory":...}]|2024-11-15 09:12:00  |null         |
    # |5UmnRwIxZyJLAsasanu8  |AT          |N         |null                 |2024-12-01 14:30:00  |null         |
    # |23444                 |DE          |N         |[{"tagCategory":...}]|2024-11-28 07:45:00  |null         |
    # +----------------------+------------+----------+---------------------+---------------------+-------------+
    # ~180,000 rows | tagCategories=null for players with no active tags (expected for LEFT JOIN)
    # Also carries: userId, countryCode, birthDate, city, email, firstName, gender, lastName,
    #               postcode, address1, birthCity, maidenName, nationality, phone, createdAt

    # -------------------------------------------------------------------------
    # Step 6: build JSON payload column using the UDF
    # One row per player → one payload_json ready for POST
    # -------------------------------------------------------------------------
    final = joined.withColumn(
        "payload_json",
        build_payload_udf(
            struct(
                *[
                    col(c)
                    for c in [
                        "userId",
                        "countryCode",
                        "birthDate",
                        "city",
                        "email",
                        "firstName",
                        "gender",
                        "lastName",
                        "postcode",
                        "address1",
                        "birthCity",
                        "maidenName",
                        "nationality",
                        "phone",
                        "createdAt",
                        "isTestUser",
                        "tagCategories",
                    ]
                ]
            )
        ),
    ).select(
        "online_customer_id",
        "country_code",
        "payload_json",
        "source_updated_at",
        "tag_updated_at",
        "isTestUser",  # keep for dbt tests / quick inspection
        "dispatched_at",
        "dispatch_error",
    )

    # final.show(2, truncate=60) — dispatcher-ready output (8 cols):
    # +----------------------+------------+------------------------------------------------------------+---------------------+---------------------+----------+-------------+--------------+
    # |online_customer_id    |country_code|payload_json                                                |source_updated_at    |tag_updated_at       |isTestUser|dispatched_at|dispatch_error|
    # +----------------------+------------+------------------------------------------------------------+---------------------+---------------------+----------+-------------+--------------+
    # |222217                |DE          |{"collection":"Account","onlineCustomerId":"222217",...      |2024-11-15 09:12:00  |2024-11-20 10:00:00  |Y         |null         |null          |
    # |5UmnRwIxZyJLAsasanu8  |AT          |{"collection":"Account","onlineCustomerId":"Kef6h5U...      |2024-12-01 14:30:00  |null                 |N         |null         |null          |
    # +----------------------+------------+------------------------------------------------------------+---------------------+---------------------+----------+-------------+--------------+
    # ~180,000 rows | dispatched_at=null → picked up by WHERE dispatched_at IS NULL in dispatcher
    #
    # payload_json for userId=222217 (pretty-printed):
    # {
    #   "collection": "Account",
    #   "onlineCustomerId": "222217",
    #   "account": {
    #     "eventType": "signup",
    #     "birthdate": "1985-03-22",
    #     "city": "Berlin",
    #     "country": "DE",
    #     "isTestUser": "Y",
    #     "email": "max.mustermann@example.com",
    #     "firstName": "Max",
    #     "lastName": "Mustermann",
    #     "tags": [{"tagCategory": "TEST", "active": true}, {"tagCategory": "VIP", "active": true}],
    #     "createdOn": "2020-03-10T08:00:00.000Z"
    #   },
    #   "sentAt": "2024-12-07T10:00:00.000Z"
    # }

    return final
