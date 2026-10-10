#!/usr/bin/env python3
"""
Spark Direct Batch Archive Loader -> Apache Iceberg
Reads staged GDELT 2.0 daily event CSVs from MinIO landing / local staging,
parses the 58+ tab-delimited columns with explicit schemas, converts FIPS country
codes to ISO-3 across all 240+ nations, extracts precise timestamps from DATEADDED,
and bulk-loads idempotently via ACID MERGE INTO directly into iceberg.silver_gdelt.events
partitioned by year and country.
"""

import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, DoubleType, IntegerType
import pyspark.sql.functions as F

STAGING_DIR = os.environ.get("STAGING_DIR", "/tmp/lakehouse/landing/gdelt")

# Comprehensive FIPS 10-4 to ISO-3 alpha-3 lookup mapping (243 nations & territories)
FIPS_MAP = {
    "AF": "AFG", "AX": "ALA", "AL": "ALB", "AG": "DZA", "AQ": "ASM", "AN": "AND", "AO": "AGO", "AV": "AIA",
    "AY": "ATA", "AC": "ATG", "AR": "ARG", "AM": "ARM", "AA": "ABW", "AS": "AUS", "AU": "AUT", "AJ": "AZE",
    "BF": "BHS", "BA": "BHR", "BG": "BGD", "BB": "BRB", "BO": "BLR", "BE": "BEL", "BH": "BLZ", "BN": "BEN",
    "BD": "BMU", "BT": "BTN", "BL": "BOL", "BK": "BIH", "BC": "BWA", "BV": "BVT", "BR": "BRA", "IO": "IOT",
    "BX": "BRN", "BU": "BGR", "UV": "BFA", "BM": "MMR", "BY": "BDI", "CB": "KHM", "CM": "CMR", "CA": "CAN",
    "CV": "CPV", "CJ": "CYM", "CT": "CAF", "CD": "TCD", "CI": "CHL", "CH": "CHN", "KT": "CXR", "CK": "CCK",
    "CO": "COL", "CN": "COM", "CG": "COD", "CF": "COG", "CW": "COK", "CS": "CRI", "IV": "CIV", "HR": "HRV",
    "CU": "CUB", "CY": "CYP", "EZ": "CZE", "DA": "DNK", "DJ": "DJI", "DO": "DMA", "DR": "DOM", "EC": "ECU",
    "EG": "EGY", "ES": "SLV", "EK": "GNQ", "ER": "ERI", "EN": "EST", "ET": "ETH", "FK": "FLK", "FO": "FRO",
    "FJ": "FJI", "FI": "FIN", "FR": "FRA", "FP": "PYF", "FS": "ATF", "GB": "GAB", "GA": "GMB", "GG": "GEO",
    "GM": "DEU", "GH": "GHA", "GI": "GIB", "GR": "GRC", "GL": "GRL", "GJ": "GRD", "GP": "GLP", "GQ": "GUM",
    "GT": "GTM", "GK": "GGY", "GV": "GIN", "PU": "GNB", "GY": "GUY", "HA": "HTI", "HM": "HMD", "VT": "VAT",
    "HO": "HND", "HK": "HKG", "HU": "HUN", "IC": "ISL", "IN": "IND", "ID": "IDN", "IR": "IRN", "IZ": "IRQ",
    "EI": "IRL", "IS": "ISR", "IT": "ITA", "JM": "JAM", "JA": "JPN", "JE": "JEY", "JO": "JOR", "KZ": "KAZ",
    "KE": "KEN", "KR": "KIR", "KN": "PRK", "KS": "KOR", "KU": "KWT", "KG": "KGZ", "LA": "LAO", "LG": "LVA",
    "LE": "LBN", "LT": "LSO", "LI": "LBR", "LY": "LBY", "LS": "LIE", "LH": "LTU", "LU": "LUX", "MC": "MAC",
    "MK": "MKD", "MA": "MDG", "MI": "MWI", "MY": "MYS", "MV": "MDV", "ML": "MLI", "MT": "MLT", "IM": "IMN",
    "RM": "MHL", "MB": "MTQ", "MR": "MRT", "MP": "MUS", "MF": "MYT", "MX": "MEX", "FM": "FSM", "MD": "MDA",
    "MN": "MCO", "MG": "MNG", "MJ": "MNE", "MH": "MSR", "MO": "MAR", "MZ": "MOZ", "WA": "NAM", "NR": "NRU",
    "NP": "NPL", "NL": "NLD", "NC": "NCL", "NZ": "NZL", "NU": "NIC", "NG": "NER", "NI": "NGA", "NE": "NIU",
    "NF": "NFK", "CQ": "MNP", "NO": "NOR", "MU": "OMN", "PK": "PAK", "PS": "PLW", "WE": "PSE", "GZ": "PSE",
    "PM": "PAN", "PP": "PNG", "PF": "PRY", "PE": "PER", "RP": "PHL", "PC": "PCN", "PL": "POL", "PO": "PRT",
    "RQ": "PRI", "QA": "QAT", "RO": "ROU", "RS": "RUS", "RW": "RWA", "RE": "REU", "BL": "BLM", "SH": "SHN",
    "SC": "KNA", "ST": "LCA", "RN": "MAF", "SB": "SPM", "VC": "VCT", "WS": "WSM", "SM": "SMR", "TP": "STP",
    "SA": "SAU", "SG": "SEN", "RI": "SRB", "SE": "SYC", "SL": "SLE", "SN": "SGP", "SX": "SXM", "LO": "SVK",
    "SI": "SVN", "BP": "SLB", "SO": "SOM", "SF": "ZAF", "SX": "SGS", "SS": "SSD", "SP": "ESP", "CE": "LKA",
    "SU": "SDN", "NS": "SUR", "SV": "SJM", "WZ": "SWZ", "SW": "SWE", "SZ": "CHE", "SY": "SYR", "TW": "TWN",
    "TI": "TJK", "TZ": "TZA", "TH": "THA", "TT": "TLS", "TO": "TGO", "TL": "TKL", "TN": "TON", "TD": "TTO",
    "TS": "TUN", "TU": "TUR", "TX": "TKM", "TK": "TCA", "TV": "TUV", "UG": "UGA", "UP": "UKR", "AE": "ARE",
    "UK": "GBR", "US": "USA", "UY": "URY", "UZ": "UZB", "NH": "VUT", "VE": "VEN", "VM": "VNM", "VQ": "VIR",
    "WQ": "WLF", "WI": "ESH", "YM": "YEM", "ZA": "ZMB", "ZI": "ZWE"
}

def get_spark_session():
    return SparkSession.builder \
        .appName("LoadGDELTArchivesToIceberg") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

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

def main(spark=None):
    print("=" * 70)
    print("  SPARK GDELT BATCH LOADER -> APACHE ICEBERG (IDEMPOTENT MERGE)")
    print("=" * 70)

    if spark is None:
        spark = get_spark_session()
    ensure_silver_tables(spark)

    csv_pattern = os.path.join(STAGING_DIR, "extracted", "*", "*.CSV")
    matched_files = glob.glob(csv_pattern) + glob.glob(os.path.join(STAGING_DIR, "extracted", "*", "*.csv"))

    if not matched_files:
        print(f"[*] No extracted GDELT CSV files found in {csv_pattern}. Searching landing...")
        csv_pattern = "/tmp/lakehouse/landing/gdelt/*.CSV"
        matched_files = glob.glob(csv_pattern)

    if not matched_files:
        print(f"[-] No GDELT CSV files found matching {csv_pattern}. Exiting.")
        return

    print(f"[*] Found {len(matched_files)} GDELT CSV archive files. Loading in parallel...")

    # Read tab-delimited headerless CSV as raw text or standard delimited format
    df_raw = spark.read \
        .option("delimiter", "\t") \
        .option("header", "false") \
        .option("inferSchema", "false") \
        .csv(matched_files)

    print(f"[*] Total raw records read: {df_raw.count()}")

    # Build mapping expression for FIPS -> ISO-3
    mapping_expr = F.create_map([F.lit(x) for x in sum(FIPS_MAP.items(), ())])

    # Normalize fields from GDELT 2.0 column positions
    df_clean = df_raw \
        .filter(F.col("_c0").isNotNull() & (F.length(F.col("_c0")) > 0)) \
        .withColumn("global_event_id", F.col("_c0").cast(LongType())) \
        .withColumn("event_date", F.to_date(F.col("_c1"), "yyyyMMdd")) \
        .withColumn("actor1_name", F.coalesce(F.col("_c6"), F.lit(""))) \
        .withColumn("actor1_country", F.coalesce(F.col("_c7"), F.lit(""))) \
        .withColumn("actor2_name", F.coalesce(F.col("_c16"), F.lit(""))) \
        .withColumn("actor2_country", F.coalesce(F.col("_c17"), F.lit(""))) \
        .withColumn("event_code", F.when(F.length(F.trim(F.coalesce(F.col("_c26"), F.lit("")))) > 0, F.trim(F.col("_c26"))).otherwise(F.lit(None).cast("string"))) \
        .withColumn("quad_class", F.when(F.length(F.trim(F.coalesce(F.col("_c29"), F.lit("")))) > 0, F.col("_c29").cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))) \
        .withColumn("goldstein_scale", F.when(F.length(F.trim(F.coalesce(F.col("_c30"), F.lit("")))) > 0, F.col("_c30").cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))) \
        .withColumn("num_mentions", F.when(F.length(F.trim(F.coalesce(F.col("_c31"), F.lit("")))) > 0, F.col("_c31").cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))) \
        .withColumn("num_sources", F.when(F.length(F.trim(F.coalesce(F.col("_c32"), F.lit("")))) > 0, F.col("_c32").cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))) \
        .withColumn("num_articles", F.when(F.length(F.trim(F.coalesce(F.col("_c33"), F.lit("")))) > 0, F.col("_c33").cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))) \
        .withColumn("avg_tone", F.when(F.length(F.trim(F.coalesce(F.col("_c34"), F.lit("")))) > 0, F.col("_c34").cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))) \
        .withColumn("fips_country", F.upper(F.trim(F.coalesce(F.col("_c53"), F.lit(""))))) \
        .withColumn(
            "mapped_country",
            F.coalesce(
                mapping_expr[F.col("fips_country")],
                F.when(F.length(F.trim(F.col("actor1_country"))) == 3, F.upper(F.trim(F.col("actor1_country")))),
                mapping_expr[F.upper(F.trim(F.col("actor2_country")))],
                F.lit("WLD")
            )
        ) \
        .withColumn(
            "action_country",
            F.when(
                F.col("mapped_country").isNotNull() & (F.length(F.trim(F.col("mapped_country"))) == 3) & (F.trim(F.col("mapped_country")) != ""),
                F.upper(F.trim(F.col("mapped_country")))
            ).otherwise(F.lit("WLD"))
        ) \
        .withColumn("latitude", F.when(F.length(F.trim(F.coalesce(F.col("_c56"), F.lit("")))) > 0, F.col("_c56").cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))) \
        .withColumn("longitude", F.when(F.length(F.trim(F.coalesce(F.col("_c57"), F.lit("")))) > 0, F.col("_c57").cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))) \
        .withColumn(
            "event_time",
            F.coalesce(
                F.to_timestamp(F.col("_c59"), "yyyyMMddHHmmss"),
                F.to_timestamp(F.col("_c1"), "yyyyMMdd")
            )
        ) \
        .withColumn("source_url", F.coalesce(F.col("_c60"), F.lit(""))) \
        .withColumn("source", F.lit("archive")) \
        .withColumn("_insertedTS", F.col("event_time")) \
        .withColumn("_modifiedTS", F.col("event_time")) \
        .withColumn("synced_at", F.current_timestamp()) \
        .select(
            "global_event_id", "event_date", "event_time", "actor1_name", "actor1_country",
            "actor2_name", "actor2_country", "event_code", "quad_class", "goldstein_scale",
            "num_mentions", "num_sources", "num_articles", "avg_tone", "action_country",
            "latitude", "longitude", "source_url", "source", "_insertedTS", "_modifiedTS", "synced_at"
        ) \
        .dropDuplicates(["global_event_id"])

    print(f"[*] Executing idempotent ACID MERGE INTO iceberg.silver_gdelt.events...")

    df_clean.createOrReplaceTempView("staged_batch")

    min_date_row = df_clean.select(F.min("event_date")).collect()[0]
    min_event_date = min_date_row[0] or "1970-01-01"

    spark.sql(f"""
        MERGE INTO iceberg.silver_gdelt.events AS target
        USING staged_batch AS source
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

    # Update sync_state watermark under dedicated 'batch_backfill' key
    # Does NOT advance 'mongo_sync' cursor_modified_ts, preventing skipped MongoDB records
    max_stats = df_clean.agg(
        F.max("global_event_id").alias("max_id"),
        F.max("event_time").alias("max_time")
    ).collect()[0]

    max_id = max_stats["max_id"] or 0
    max_time = max_stats["max_time"]

    print(f"[+] Committed batch backfill watermark: max_event_id={max_id}")

    spark.sql(f"""
        INSERT INTO iceberg.silver_gdelt.sync_state
        VALUES ('batch_backfill', TIMESTAMP '1970-01-01 00:00:00', {max_id}, TIMESTAMP '{max_time}', current_timestamp())
    """)

    print("=" * 70)
    print("  GDELT BATCH ARCHIVE BULK LOAD COMPLETE (IDEMPOTENT MERGE INTO ICEBERG)")
    print("=" * 70)

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        main(spark)
    finally:
        spark.stop()
