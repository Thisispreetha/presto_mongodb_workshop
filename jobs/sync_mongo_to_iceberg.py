#!/usr/bin/env python3
"""
Spark Incremental Micro-Batch Sync: MongoDB -> Apache Iceberg
Reads hot operational updates from MongoDB (market_data.events) using a high-watermark
_modifiedTS cursor, executes partition-pruned ACID MERGE INTO on iceberg.silver_gdelt.events,
commits progress to sync_state, and refreshes daily Gold stability tables.
"""

import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/?replicaSet=rs0")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE", "market_data")

def get_spark_session():
    return SparkSession.builder \
        .appName("SyncMongoToIceberg") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def get_last_sync_state(spark):
    try:
        df_state = spark.sql("""
            SELECT max_by(cursor_modified_ts, synced_at) as last_cursor,
                   max_by(max_event_id, synced_at) as max_id
            FROM iceberg.silver_gdelt.sync_state
            WHERE table_name = 'mongo_sync'
        """).collect()
        if df_state and df_state[0]["last_cursor"] is not None:
            return df_state[0]["last_cursor"], df_state[0]["max_id"]
    except Exception as e:
        print(f"[*] sync_state not yet initialized: {e}")
    return datetime(1970, 1, 1, tzinfo=timezone.utc), 0

def ensure_silver_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.silver_gdelt")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver_gdelt.events (
            global_event_id   BIGINT,
            event_date        DATE,
            event_time        TIMESTAMP,
            actor1_name       STRING,
            actor1_country    STRING,
            actor2_name       STRING,
            actor2_country    STRING,
            event_code        STRING,
            quad_class        INT,
            goldstein_scale   DOUBLE,
            num_mentions      INT,
            num_sources       INT,
            num_articles      INT,
            avg_tone          DOUBLE,
            action_country    STRING,
            latitude          DOUBLE,
            longitude         DOUBLE,
            source_url        STRING,
            source            STRING,
            _insertedTS       TIMESTAMP,
            _modifiedTS       TIMESTAMP,
            synced_at         TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (years(event_date), action_country)
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.merge.mode'                = 'merge-on-read',
            'write.update.mode'               = 'merge-on-read',
            'write.delete.mode'               = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd',
            'write.distribution-mode'         = 'hash'
        )
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver_gdelt.sync_state (
            table_name             STRING,
            cursor_modified_ts     TIMESTAMP,
            max_event_id           BIGINT,
            max_event_time         TIMESTAMP,
            synced_at              TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2'
        )
    """)
    # Purge any legacy 'events' cursor rows to prevent picking up obsolete batch timestamps
    try:
        spark.sql("DELETE FROM iceberg.silver_gdelt.sync_state WHERE table_name = 'events'")
    except Exception:
        pass

def ensure_gold_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.gold_gdelt")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_gdelt.country_stability_daily (
            action_country         STRING,
            event_date             DATE,
            total_events           BIGINT,
            conflict_events        BIGINT,
            cooperation_events     BIGINT,
            mean_goldstein_scale   DOUBLE,
            mean_sentiment_tone    DOUBLE,
            total_mentions         BIGINT,
            total_sources          BIGINT,
            high_impact_crises     BIGINT
        )
        USING iceberg
        PARTITIONED BY (years(event_date))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)

def main(spark=None):
    print("=" * 70)
    print("  SPARK INCREMENTAL SYNC: MONGODB -> APACHE ICEBERG")
    print("=" * 70)

    if spark is None:
        spark = get_spark_session()
    ensure_silver_tables(spark)
    ensure_gold_tables(spark)
    last_cursor, max_id = get_last_sync_state(spark)
    print(f"[*] Last committed sync watermark: cursor={last_cursor}, max_id={max_id}")

    # Read from MongoDB using pushdown aggregation pipeline
    # Read delta records where _modifiedTS > last_cursor (or all recent if cursor is epoch)
    print(f"[*] Querying MongoDB {MONGO_DATABASE}.events delta records...")
    df_mongo = spark.read \
        .format("mongodb") \
        .option("database", MONGO_DATABASE) \
        .option("collection", "events") \
        .load()

    # Filter changes based on _modifiedTS
    if last_cursor and last_cursor.year > 1970:
        df_delta = df_mongo.filter(F.col("_modifiedTS") >= F.lit(last_cursor))
    else:
        df_delta = df_mongo

    delta_count = df_delta.count()
    print(f"[*] Found {delta_count} modified/new records in MongoDB hot tier.")

    if delta_count == 0:
        print("[+] Zero delta records to sync. Silver layer is up to date.")
        return

    # Normalize types for Iceberg Silver table
    df_normalized = df_delta \
        .withColumn("global_event_id", F.col("global_event_id").cast("bigint")) \
        .withColumn("event_date", F.col("event_date").cast("date")) \
        .withColumn("event_time", F.col("event_time").cast("timestamp")) \
        .withColumn("actor1_name", F.coalesce(F.col("actor1_name"), F.lit(""))) \
        .withColumn("actor1_country", F.coalesce(F.col("actor1_country"), F.lit(""))) \
        .withColumn("actor2_name", F.coalesce(F.col("actor2_name"), F.lit(""))) \
        .withColumn("actor2_country", F.coalesce(F.col("actor2_country"), F.lit(""))) \
        .withColumn("event_code", F.when(F.length(F.trim(F.coalesce(F.col("event_code"), F.lit("")))) > 0, F.trim(F.col("event_code"))).otherwise(F.lit(None).cast("string"))) \
        .withColumn("quad_class", F.col("quad_class").cast("int")) \
        .withColumn("goldstein_scale", F.col("goldstein_scale").cast("double")) \
        .withColumn("num_mentions", F.col("num_mentions").cast("int")) \
        .withColumn("num_sources", F.col("num_sources").cast("int")) \
        .withColumn("num_articles", F.col("num_articles").cast("int")) \
        .withColumn("avg_tone", F.col("avg_tone").cast("double")) \
        .withColumn(
            "action_country",
            F.when(
                F.col("action_country").isNotNull() & (F.length(F.trim(F.col("action_country"))) == 3) & (F.trim(F.col("action_country")) != ""),
                F.upper(F.trim(F.col("action_country")))
            ).otherwise(F.lit("WLD"))
        ) \
        .withColumn("latitude", F.col("latitude").cast("double")) \
        .withColumn("longitude", F.col("longitude").cast("double")) \
        .withColumn("source_url", F.coalesce(F.col("source_url"), F.lit(""))) \
        .withColumn("source", F.lit("stream")) \
        .withColumn("_insertedTS", F.coalesce(F.col("_insertedTS").cast("timestamp"), F.current_timestamp())) \
        .withColumn("_modifiedTS", F.coalesce(F.col("_modifiedTS").cast("timestamp"), F.current_timestamp())) \
        .dropDuplicates(["global_event_id"])

    df_normalized.createOrReplaceTempView("staged_updates")

    # Determine minimum event date to enable partition pruning in MERGE INTO
    min_date_row = df_normalized.select(F.min("event_date")).collect()[0]
    min_event_date = min_date_row[0] or "1970-01-01"

    print(f"[*] Executing partition-pruned ACID MERGE INTO (min_event_date={min_event_date})...")

    spark.sql(f"""
        MERGE INTO iceberg.silver_gdelt.events AS target
        USING staged_updates AS source
        ON target.event_date >= DATE '{min_event_date}'
           AND target.global_event_id = source.global_event_id
        WHEN MATCHED AND source._modifiedTS > target._modifiedTS THEN UPDATE SET
            target.num_mentions = source.num_mentions,
            target.num_sources = source.num_sources,
            target.num_articles = source.num_articles,
            target.avg_tone = source.avg_tone,
            target.latitude = source.latitude,
            target.longitude = source.longitude,
            target._modifiedTS = source._modifiedTS,
            target.synced_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            global_event_id, event_date, event_time, actor1_name, actor1_country,
            actor2_name, actor2_country, event_code, quad_class, goldstein_scale,
            num_mentions, num_sources, num_articles, avg_tone, action_country,
            latitude, longitude, source_url, source, _insertedTS, _modifiedTS, synced_at
        ) VALUES (
            source.global_event_id, source.event_date, source.event_time, source.actor1_name, source.actor1_country,
            source.actor2_name, source.actor2_country, source.event_code, source.quad_class, source.goldstein_scale,
            source.num_mentions, source.num_sources, source.num_articles, source.avg_tone, source.action_country,
            source.latitude, source.longitude, source.source_url, source.source, source._insertedTS, source._modifiedTS, current_timestamp()
        )
    """)

    # Compute new watermarks
    watermarks = df_normalized.agg(
        F.max("_modifiedTS").alias("new_cursor"),
        F.max("global_event_id").alias("new_max_id"),
        F.max("event_time").alias("new_max_time")
    ).collect()[0]

    new_cursor = watermarks["new_cursor"]
    new_max_id = watermarks["new_max_id"] or max_id
    new_max_time = watermarks["new_max_time"] or datetime.now(timezone.utc)

    spark.sql(f"""
        INSERT INTO iceberg.silver_gdelt.sync_state
        VALUES ('mongo_sync', TIMESTAMP '{new_cursor}', {new_max_id}, TIMESTAMP '{new_max_time}', current_timestamp())
    """)

    print(f"[+] Committed new sync watermark: max_event_id={new_max_id}, cursor={new_cursor}")

    # Idempotently update recent 7 days of gold_gdelt.country_stability_daily via MERGE INTO
    # Preserves historical 90-day baseline data without year-partition wiping
    print("[*] Refreshing Gold country_stability_daily table via MERGE INTO...")
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW staged_gold_stability AS
        SELECT
            action_country,
            event_date,
            COUNT(*) AS total_events,
            COUNT(CASE WHEN quad_class IN (3, 4) THEN 1 END) AS conflict_events,
            COUNT(CASE WHEN quad_class IN (1, 2) THEN 1 END) AS cooperation_events,
            ROUND(AVG(goldstein_scale), 3) AS mean_goldstein_scale,
            ROUND(AVG(avg_tone), 3) AS mean_sentiment_tone,
            SUM(num_mentions) AS total_mentions,
            SUM(num_sources) AS total_sources,
            COUNT(CASE WHEN goldstein_scale <= -5.0 AND num_mentions >= 50 THEN 1 END) AS high_impact_crises
        FROM iceberg.silver_gdelt.events
        WHERE event_date >= CURRENT_DATE - INTERVAL '7' DAY
        GROUP BY action_country, event_date
    """)

    spark.sql("""
        MERGE INTO iceberg.gold_gdelt.country_stability_daily AS target
        USING staged_gold_stability AS source
        ON target.action_country = source.action_country
           AND target.event_date = source.event_date
        WHEN MATCHED THEN UPDATE SET
            target.total_events = source.total_events,
            target.conflict_events = source.conflict_events,
            target.cooperation_events = source.cooperation_events,
            target.mean_goldstein_scale = source.mean_goldstein_scale,
            target.mean_sentiment_tone = source.mean_sentiment_tone,
            target.total_mentions = source.total_mentions,
            target.total_sources = source.total_sources,
            target.high_impact_crises = source.high_impact_crises
        WHEN NOT MATCHED THEN INSERT (
            action_country, event_date, total_events, conflict_events, cooperation_events,
            mean_goldstein_scale, mean_sentiment_tone, total_mentions, total_sources, high_impact_crises
        ) VALUES (
            source.action_country, source.event_date, source.total_events, source.conflict_events, source.cooperation_events,
            source.mean_goldstein_scale, source.mean_sentiment_tone, source.total_mentions, source.total_sources, source.high_impact_crises
        )
    """)

    print("=" * 70)
    print("  INCREMENTAL LAKEHOUSE SYNC & GOLD REFRESH COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        main(spark)
    finally:
        spark.stop()
