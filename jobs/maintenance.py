#!/usr/bin/env python3
"""
Apache Iceberg Table Maintenance Job
Performs binpack file compaction, snapshot expiration, and orphan file cleanup
on iceberg.silver_gdelt.events to keep query planning fast and eliminate small-file bloat.
"""

from pyspark.sql import SparkSession

def main():
    print("=" * 70)
    print("  APACHE ICEBERG TABLE MAINTENANCE (GDELT LAKEHOUSE)")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName("IcebergMaintenanceGDELT") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    # 1. Binpack Compaction
    print("[*] Executing Iceberg small-file binpack compaction on silver_gdelt.events...")
    spark.sql("""
        CALL iceberg.system.rewrite_data_files(
            table => 'silver_gdelt.events',
            strategy => 'binpack',
            options => map('target-file-size-bytes', '134217728', 'min-file-size-bytes', '33554432', 'max-file-size-bytes', '268435456', 'min-input-files', '2')
        )
    """).show(truncate=False)

    from datetime import datetime, timedelta, timezone

    # 2. Snapshot Expiration (Retain 3 days of snapshots for time-travel)
    print("[*] Expiring historical snapshots older than 3 days...")
    cutoff_ts = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL iceberg.system.expire_snapshots(
            table => 'silver_gdelt.events',
            older_than => TIMESTAMP '{cutoff_ts}',
            retain_last => 5
        )
    """).show(truncate=False)

    # 3. Orphan File Removal
    print("[*] Removing orphan Parquet files...")
    orphan_cutoff = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL iceberg.system.remove_orphan_files(
            table => 'silver_gdelt.events',
            older_than => TIMESTAMP '{orphan_cutoff}'
        )
    """).show(truncate=False)

    print("=" * 70)
    print("  ICEBERG TABLE MAINTENANCE COMPLETED SUCCESSFULLY")
    print("=" * 70)

if __name__ == "__main__":
    main()
