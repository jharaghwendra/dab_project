"""Generic player activity summary dbt Python model.

This model preserves the incremental PySpark/dbt pattern while keeping the
columns and payload structure anonymized.
"""

import json
from datetime import datetime, timezone

from pyspark.sql.functions import col, collect_set, coalesce, lit, max, struct, udf
from pyspark.sql.types import StringType


def clean_str(val):
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


def build_payload(row):
    sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    created_on_str = None
    if row.createdAt is not None:
        try:
            created_on_str = row.createdAt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except AttributeError:
            created_on_str = str(row.createdAt)

    payload = {
        "collection": "PlayerSummary",
        "playerId": str(row.userId),
        "summary": {
            "eventType": "profile_refresh",
            "country": row.countryCode,
            "region": clean_str(row.regionCode),
            "accountTier": clean_str(row.accountTier),
            "playerAlias": clean_str(row.playerAlias),
            "isVerified": row.isVerified,
            "createdOn": created_on_str,
        },
        "sentAt": sent_at,
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


build_payload_udf = udf(build_payload, StringType())


def model(dbt, session):
    dbt.config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["online_customer_id", "country_code"],
        file_format="delta",
        submission_method="serverless_cluster",
    )

    dim_player = dbt.ref("dim_player").filter(col("dbt_valid_to") == lit("9999-12-31").cast("timestamp"))
    dim_tag = dbt.ref("dim_tag").filter(
        (col("dbt_valid_to") == lit("9999-12-31").cast("timestamp")) & (col("targetType") == "User") & col("active")
    )

    if dbt.is_incremental:
        max_seen = session.sql(f"SELECT COALESCE(MAX(source_updated_at), '1970-01-01') FROM {dbt.this}").collect()[0][0]
        dim_player = dim_player.filter(col("updatedAt") > lit(max_seen))
        changed_tag_users = (
            dim_tag.filter(col("dbt_updated_at") > lit(max_seen))
            .select(col("targetId").alias("player_id"), col("country_code"))
            .distinct()
        )
        dim_player = dim_player.union(
            dbt.ref("dim_player")
            .filter(col("dbt_valid_to") == lit("9999-12-31").cast("timestamp"))
            .join(changed_tag_users, on=["player_id", "country_code"], how="inner")
        ).distinct()

    tag_agg = dim_tag.groupBy("targetId", "country_code").agg(
        collect_set(
            struct(
                col("tagCategory").alias("tagCategory"),
                col("active").alias("active"),
            )
        ).alias("tagCategories"),
        max(col("dbt_updated_at")).alias("tag_updated_at"),
    )

    joined = dim_player.join(
        tag_agg,
        on=[
            dim_player["player_id"] == tag_agg["targetId"],
            dim_player["country_code"] == tag_agg["country_code"],
        ],
        how="left",
    ).select(
        dim_player["player_id"].alias("online_customer_id"),
        dim_player["country_code"],
        dim_player["userId"],
        dim_player["countryCode"],
        dim_player["createdAt"],
        dim_player["playerAlias"],
        dim_player["regionCode"],
        dim_player["accountTier"],
        dim_player["isVerified"],
        dim_player["updatedAt"].alias("source_updated_at"),
        tag_agg["tag_updated_at"],
        coalesce(tag_agg["tagCategories"], lit(None)).alias("tagCategories"),
        lit(None).cast("timestamp").alias("dispatched_at"),
        lit(None).cast("string").alias("dispatch_error"),
    )

    final = joined.withColumn(
        "payload_json",
        build_payload_udf(
            struct(
                *[
                    col(c)
                    for c in [
                        "userId",
                        "countryCode",
                        "createdAt",
                        "playerAlias",
                        "regionCode",
                        "accountTier",
                        "isVerified",
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
        "dispatched_at",
        "dispatch_error",
    )

    return final
