# =============================================================================
# nb_pc_dispatcher.py — PowerComply Account Signup Dispatcher
# Phase 3 of the PowerComply integration — Layer 2 (HTTP Dispatcher)
#
# ROLE IN 2-LAYER ARCHITECTURE:
#   Layer 1 — dbt Python model (dbt_gold/models/marts/mart_pc_account_signup.py)
#     Reads dim_player + dim_tag, builds payload_json, stages in Delta.
#     dispatched_at = NULL on every new/updated row.
#
#   Layer 2 — THIS SCRIPT
#     Reads WHERE dispatched_at IS NULL, POSTs to PowerComply, marks sent/failed.
#
# REPLACES TALEND:
#   Refresh_Token_If_Expired  →  get_pc_token() with TTLCache
#   RestClient_To_Post_Data   →  post_to_powercomply() + retry logic
#   Job control table update  →  DeltaTable.update() per row
#
# DEAD-LETTER PATTERN:
#   dispatched_at != NULL + dispatch_error = NULL  → sent successfully
#   dispatched_at = NULL  + dispatch_error != NULL → failed, investigate
#   One failed row does NOT stop the loop — continues to next row.
#
# RUN CADENCE:
#   Triggered by Databricks Workflow after dbt run --select mart_pc_account_signup.
#   Recommended: every 10 minutes, or chained as next task in same Workflow.
# =============================================================================

import sys
import time
from datetime import datetime, timezone

import requests
from cachetools import TTLCache, cached
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

# =============================================================================
# CONFIGURATION
# All sensitive values stored in Databricks secret scope "powercomply".
# Set up once:
#   databricks secrets create-scope --scope powercomply
#   databricks secrets put-secret --scope powercomply --key token_url
#   databricks secrets put-secret --scope powercomply --key api_base_url
#   databricks secrets put-secret --scope powercomply --key client_id
#   databricks secrets put-secret --scope powercomply --key client_secret
# =============================================================================

CATALOG = "tma_dev"
SCHEMA = "gold"
TABLE = "mart_pc_account_signup"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

# PowerComply API — all sensitive values from secret scope, never hardcoded
PC_TOKEN_URL = dbutils.secrets.get(scope="powercomply", key="token_url")
PC_API_BASE_URL = dbutils.secrets.get(scope="powercomply", key="api_base_url")
PC_CLIENT_ID = dbutils.secrets.get(scope="powercomply", key="client_id")
PC_CLIENT_SECRET = dbutils.secrets.get(scope="powercomply", key="client_secret")

# Dispatcher tuning
REQUEST_TIMEOUT_SECS = 30  # per-request HTTP timeout
POST_DELAY_SECS = 0.1  # sleep between rows — rate limiting for PC API
MAX_RETRIES = 3  # retry attempts per row on network / 5xx errors
RETRY_BACKOFF_SECS = 2  # base sleep: 2s → 4s → 8s (doubles each attempt)
LOG_EVERY_N_ROWS = 100  # print progress summary every N rows

# =============================================================================
# TOKEN — TTLCache decorator (replaces Talend Refresh_Token_If_Expired)
#
# HOW TTLCache WORKS vs manual class:
#
#   Manual class (old):
#     - Manually tracked time.time() vs _expires_at, ~30 lines of boilerplate
#     - Required explicit 60s buffer logic and _refresh() calls
#
#   TTLCache decorator (this file):
#     - @cached(cache=TTLCache(maxsize=1, ttl=3540))
#     - Cache auto-expires after 3540s (59 min). Token lasts 3600s (1 hour).
#     - On expiry: next call executes function body, fetches fresh token, re-caches.
#     - No manual expiry tracking needed. Same pattern used for Salesforce tokens.
#
#   maxsize=1 (not 300 like Salesforce):
#     - maxsize=300 makes sense when called with different credentials per tenant.
#     - Here we only ever call with one set of credentials → maxsize=1 is correct.
#
#   On 401/403 mid-run (token rejected before TTL expires):
#     - Call get_pc_token.cache.clear() to evict the cached token immediately.
#     - Next call to get_pc_token() fetches a fresh token regardless of TTL.
# =============================================================================


@cached(cache=TTLCache(maxsize=1, ttl=3540))
def get_pc_token(token_url, client_id, client_secret):
    """
    Fetch an OAuth2 bearer token for PowerComply.
    Result cached for 3540s (59 min). Auto-refreshes on cache expiry.

    Cache hit  → returns cached token immediately (no HTTP call)
    Cache miss → POST /token, cache result, return access_token
    """
    print(f"[Token] Fetching new PowerComply token at {_now_utc()}")
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=REQUEST_TIMEOUT_SECS,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("[Token] Token fetched. Cached for 3540s (next refresh ~59min from now).")
    return token


# =============================================================================
# HELPERS
# =============================================================================


def _now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def post_to_powercomply(payload_json):
    """
    POST one JSON payload to PowerComply Account endpoint.
    Token fetched via get_pc_token() — TTLCache handles refresh automatically.

    Retry logic:
      200         → success
      401 / 403   → clear cache to force token refresh, retry
      4xx (other) → dead-letter immediately, no retry (payload issue)
      5xx         → retry MAX_RETRIES with exponential backoff (2s, 4s, 8s)
      Timeout     → retry MAX_RETRIES

    Returns: (success: bool, status_code: int, error_body: str | None)
    """
    url = f"{PC_API_BASE_URL}/api/v1/events"

    for attempt in range(1, MAX_RETRIES + 1):
        token = get_pc_token(PC_TOKEN_URL, PC_CLIENT_ID, PC_CLIENT_SECRET)
        try:
            resp = requests.post(
                url,
                data=payload_json,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=REQUEST_TIMEOUT_SECS,
            )

            if resp.status_code == 200:
                return True, 200, None

            elif resp.status_code in (401, 403):
                # Token rejected — evict cache so next get_pc_token() refetches
                get_pc_token.cache.clear()
                if attempt < MAX_RETRIES:
                    continue
                return False, resp.status_code, resp.text[:2000]

            elif 400 <= resp.status_code < 500:
                # Client / validation error — payload issue, do NOT retry
                return False, resp.status_code, resp.text[:2000]

            else:
                # 5xx server error — retry with exponential backoff
                if attempt < MAX_RETRIES:
                    sleep_secs = RETRY_BACKOFF_SECS * (2 ** (attempt - 1))
                    print(f"  [attempt {attempt}] Server error {resp.status_code}. Retrying in {sleep_secs}s")
                    time.sleep(sleep_secs)
                    continue
                return False, resp.status_code, resp.text[:2000]

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECS * (2 ** (attempt - 1)))
            else:
                return False, 0, f"Timeout after {MAX_RETRIES} attempts ({REQUEST_TIMEOUT_SECS}s each)"

        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECS * (2 ** (attempt - 1)))
            else:
                return False, 0, f"Network error after {MAX_RETRIES} attempts: {exc}"

    return False, 0, f"Max retries ({MAX_RETRIES}) exhausted"


def mark_dispatched(delta_tbl, online_customer_id, country_code):
    """
    On HTTP 200: set dispatched_at = now(), clear dispatch_error.
    Uses DeltaTable.update() with lit() — not spark.sql(f-string) — to avoid
    SQL injection risk on user-controlled online_customer_id values.
    """
    delta_tbl.update(
        condition=((col("online_customer_id") == lit(online_customer_id)) & (col("country_code") == lit(country_code))),
        set={
            "dispatched_at": current_timestamp(),
            "dispatch_error": lit(None).cast("string"),
        },
    )


def mark_failed(delta_tbl, online_customer_id, country_code, error_body):
    """
    On non-200: write truncated error body to dispatch_error.
    dispatched_at stays NULL → row remains in dead-letter queue.
    dbt mart model resets both columns via merge when player data changes.
    """
    delta_tbl.update(
        condition=((col("online_customer_id") == lit(online_customer_id)) & (col("country_code") == lit(country_code))),
        set={
            "dispatch_error": lit(str(error_body)[:2000]),
        },
    )


# =============================================================================
# STEP 1: Read pending rows
#
# Reads all rows where dispatched_at IS NULL, ordered oldest-first.
# Collected to Pandas because iteration is row-by-row
# (PowerComply does not accept batch POST).
#
# pending_df shape (typical 10-min incremental run):
#   +----------------------+------------+-------------------------------------+
#   | online_customer_id   |country_code| payload_json                        |
#   +----------------------+------------+-------------------------------------+
#   | 528917               | DE         | {"collection":"Account","online...  |
#   | 71829                | AT         | {"collection":"Account","online...  |
#   +----------------------+------------+-------------------------------------+
#   ~50-500 rows typical. First-ever run: ~180,000 rows.
# =============================================================================

pending_df = spark.sql(f"""
    SELECT online_customer_id, country_code, payload_json
    FROM {FULL_TABLE_NAME}
    WHERE dispatched_at IS NULL
    ORDER BY source_updated_at ASC
""").toPandas()

total_pending = len(pending_df)
print(f"[Step 1] Pending rows to dispatch : {total_pending}")
print(f"[Step 1] Run started at           : {_now_utc()}")

if total_pending == 0:
    print("[Step 1] Nothing to dispatch. Exiting.")
    dbutils.jobs.taskValues.set(key="dispatch_result", value="NOTHING_TO_DISPATCH")
    sys.exit(0)

# =============================================================================
# STEP 2: Dispatch loop
#
# For each pending row:
#   1. get_pc_token() — TTLCache returns cached token or auto-fetches if expired
#   2. POST payload_json to PowerComply (row-by-row — their API constraint)
#   3. Immediate DeltaTable.update() per row
#
# WHY immediate update per row (not batch at end):
#   If cluster is killed at row 50,000, rows 1-49,999 are already marked
#   dispatched_at != NULL. Next run picks up from row 50,000. No double-send.
# =============================================================================

delta_tbl = DeltaTable.forName(spark, FULL_TABLE_NAME)
success_count = 0
failure_count = 0
start_time = time.time()

for idx, row in pending_df.iterrows():
    oci = row["online_customer_id"]
    cc = row["country_code"]
    payload = row["payload_json"]
    row_num = idx + 1

    # Progress heartbeat every LOG_EVERY_N_ROWS rows
    if row_num == 1 or row_num % LOG_EVERY_N_ROWS == 0:
        elapsed = time.time() - start_time
        rate = row_num / elapsed if elapsed > 0 else 0
        eta_secs = (total_pending - row_num) / rate if rate > 0 else 0
        print(
            f"[Dispatch] Row {row_num:>6}/{total_pending} | "
            f"ok={success_count:>5} err={failure_count:>3} | "
            f"{rate:.1f} rows/s | ETA {eta_secs / 60:.1f}min"
        )

    success, status_code, error_body = post_to_powercomply(payload)

    if success:
        mark_dispatched(delta_tbl, oci, cc)
        success_count += 1
    else:
        mark_failed(delta_tbl, oci, cc, error_body)
        failure_count += 1
        print(f"  [FAILED] oci={oci} cc={cc} http={status_code} | {str(error_body)[:200]}")

    if POST_DELAY_SECS > 0:
        time.sleep(POST_DELAY_SECS)

# =============================================================================
# STEP 3: Summary report
#
# Expected output after healthy incremental run:
#   ============================================================
#   DISPATCH COMPLETE — 2024-12-03T10:07:45.000Z
#   ============================================================
#     Total processed  : 237
#     Successful       : 235
#     Failed (errors)  : 2
#     Elapsed          : 28.4s
#     Avg rate         : 8.3 rows/s
#   ============================================================
#
# Dead-letter query for failed rows:
#   SELECT online_customer_id, country_code, dispatch_error
#   FROM tma_dev.gold.mart_pc_account_signup
#   WHERE dispatch_error IS NOT NULL AND dispatched_at IS NULL
# =============================================================================

elapsed_total = time.time() - start_time
sep = "=" * 60

print(f"\n{sep}")
print(f"DISPATCH COMPLETE — {_now_utc()}")
print(sep)
print(f"  Total processed  : {total_pending}")
print(f"  Successful       : {success_count}")
print(f"  Failed (errors)  : {failure_count}")
print(f"  Elapsed          : {elapsed_total:.1f}s")
print(f"  Avg rate         : {total_pending / elapsed_total:.1f} rows/s")
print(sep)

if failure_count > 0:
    print(f"\nWARNING: {failure_count} rows failed. Investigate with:")
    print(f"  SELECT online_customer_id, country_code, dispatch_error")
    print(f"  FROM {FULL_TABLE_NAME}")
    print(f"  WHERE dispatch_error IS NOT NULL AND dispatched_at IS NULL")
    print(sep)

# Pass result to downstream tasks via Databricks task values.
# Use in Workflow: Tasks → this task → taskValues → dispatch_result
# dbutils.notebook.exit() only works for Notebook task type — not Python script.
dbutils.jobs.taskValues.set(key="dispatch_result", value=f"OK: {success_count} dispatched, {failure_count} failed")
