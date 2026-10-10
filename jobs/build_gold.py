#!/usr/bin/env python3
"""
Spark Daily Gold Layer Aggregation Job
Computes multi-dimensional analytics from iceberg.silver_gdelt.events:
1. Daily country stability and sentiment profiles
2. Bilateral country conflict vs. cooperation interaction matrices
3. Weekly crisis anomaly detection Z-scores
"""

from pyspark.sql import SparkSession

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
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_gdelt.global_conflict_matrix (
            actor1_country         STRING,
            actor2_country         STRING,
            interaction_count      BIGINT,
            conflict_count         BIGINT,
            cooperation_count      BIGINT,
            avg_goldstein_score    DOUBLE,
            avg_tone_score         DOUBLE,
            total_media_coverage   BIGINT,
            last_updated_at        TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold_gdelt.crisis_hotspots_summary (
            action_country         STRING,
            week_start             DATE,
            weekly_events          BIGINT,
            weekly_conflict_ratio  DOUBLE,
            baseline_events_avg    DOUBLE,
            event_surge_zscore     DOUBLE,
            is_crisis_anomaly      BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (years(week_start))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)

def main():
    print("=" * 70)
    print("  SPARK GDELT GOLD AGGREGATION PIPELINE")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("BuildGDELTGold") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.iceberg.vectorization.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .getOrCreate()

    ensure_gold_tables(spark)

    # 1. Rebuild country_stability_daily (Idempotent 90-Day MERGE INTO - Preserves Year History)
    print("[*] Computing iceberg.gold_gdelt.country_stability_daily via MERGE INTO...")
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW staged_gold_stability_90d AS
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
        WHERE event_date >= CURRENT_DATE - INTERVAL '90' DAY
        GROUP BY action_country, event_date
    """)

    spark.sql("""
        MERGE INTO iceberg.gold_gdelt.country_stability_daily AS target
        USING staged_gold_stability_90d AS source
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

    # 2. Rebuild global_conflict_matrix (Actor1 vs Actor2 Bilateral Diplomacy)
    print("[*] Computing iceberg.gold_gdelt.global_conflict_matrix...")
    spark.sql("""
        INSERT OVERWRITE iceberg.gold_gdelt.global_conflict_matrix
        SELECT
            actor1_country,
            actor2_country,
            COUNT(*) AS interaction_count,
            COUNT(CASE WHEN quad_class IN (3, 4) THEN 1 END) AS conflict_count,
            COUNT(CASE WHEN quad_class IN (1, 2) THEN 1 END) AS cooperation_count,
            ROUND(AVG(goldstein_scale), 3) AS avg_goldstein_score,
            ROUND(AVG(avg_tone), 3) AS avg_tone_score,
            SUM(num_mentions) AS total_media_coverage,
            current_timestamp() AS last_updated_at
        FROM iceberg.silver_gdelt.events
        WHERE actor1_country != '' AND actor2_country != ''
          AND actor1_country != actor2_country
          AND event_date >= CURRENT_DATE - INTERVAL '90' DAY
        GROUP BY actor1_country, actor2_country
    """)

    # 3. Rebuild crisis_hotspots_summary (Weekly Anomaly Scores via MERGE INTO)
    print("[*] Computing iceberg.gold_gdelt.crisis_hotspots_summary via MERGE INTO...")
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW staged_crisis_hotspots AS
        WITH weekly_stats AS (
            SELECT
                action_country,
                date_trunc('week', event_date) AS week_start,
                COUNT(*) AS weekly_events,
                ROUND(COUNT(CASE WHEN quad_class IN (3, 4) THEN 1 END) * 1.0 / COUNT(*), 3) AS weekly_conflict_ratio
            FROM iceberg.silver_gdelt.events
            WHERE event_date >= CURRENT_DATE - INTERVAL '180' DAY
            GROUP BY action_country, date_trunc('week', event_date)
        ),
        baseline AS (
            SELECT
                action_country,
                AVG(weekly_events) AS baseline_events_avg,
                STDDEV_POP(weekly_events) AS baseline_stddev
            FROM weekly_stats
            GROUP BY action_country
        )
        SELECT
            w.action_country,
            CAST(w.week_start AS DATE) AS week_start,
            w.weekly_events,
            w.weekly_conflict_ratio,
            ROUND(b.baseline_events_avg, 1) AS baseline_events_avg,
            ROUND(CASE WHEN b.baseline_stddev > 0 THEN (w.weekly_events - b.baseline_events_avg) / b.baseline_stddev ELSE 0 END, 2) AS event_surge_zscore,
            CASE WHEN (w.weekly_events - b.baseline_events_avg) > (2 * b.baseline_stddev) THEN true ELSE false END AS is_crisis_anomaly
        FROM weekly_stats w
        JOIN baseline b ON w.action_country = b.action_country
    """)

    spark.sql("""
        MERGE INTO iceberg.gold_gdelt.crisis_hotspots_summary AS target
        USING staged_crisis_hotspots AS source
        ON target.action_country = source.action_country
           AND target.week_start = source.week_start
        WHEN MATCHED THEN UPDATE SET
            target.weekly_events = source.weekly_events,
            target.weekly_conflict_ratio = source.weekly_conflict_ratio,
            target.baseline_events_avg = source.baseline_events_avg,
            target.event_surge_zscore = source.event_surge_zscore,
            target.is_crisis_anomaly = source.is_crisis_anomaly
        WHEN NOT MATCHED THEN INSERT (
            action_country, week_start, weekly_events, weekly_conflict_ratio,
            baseline_events_avg, event_surge_zscore, is_crisis_anomaly
        ) VALUES (
            source.action_country, source.week_start, source.weekly_events, source.weekly_conflict_ratio,
            source.baseline_events_avg, source.event_surge_zscore, source.is_crisis_anomaly
        )
    """)

    print("=" * 70)
    print("  GOLD LAYER AGGREGATIONS BUILT SUCCESSFULLY")
    print("=" * 70)

if __name__ == "__main__":
    main()
