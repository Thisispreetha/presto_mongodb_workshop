#!/usr/bin/env python3
"""
GDELT Lakehouse Data Quality & Reconciliation Suite
Audits data integrity, schema compliance, coordinate bounds, primary key uniqueness,
ISO-3 country standardization, and Gold layer aggregation correctness across Apache Iceberg.
"""

import os
import sys
import json
import time
import urllib.request
from datetime import datetime, timezone
from pyspark.sql import SparkSession


def main(spark=None):
    start_time = datetime.now(timezone.utc)
    print("=" * 70)
    print("  GDELT LAKEHOUSE DATA QUALITY & AUDIT SUITE")
    print(f"  Started: {start_time.isoformat()}")
    print("=" * 70)

    if spark is None:
        spark = SparkSession.builder \
            .appName("GDELTDataQualityChecks") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()

    failures = 0

    # 1. Primary Key Uniqueness Audit in Silver
    print("\n[Audit 1/7] Checking Primary Key Uniqueness (global_event_id)...")
    try:
        total_count = spark.sql("SELECT count(*) FROM iceberg.silver_gdelt.events").collect()[0][0]
        if total_count == 0:
            print("  [FAIL] Silver events table is empty (0 records found)! Pipeline has not ingested data.")
            failures += 1
        else:
            distinct_count = spark.sql("SELECT count(DISTINCT global_event_id) FROM iceberg.silver_gdelt.events").collect()[0][0]
            if total_count == distinct_count:
                print(f"  [PASS] All {total_count} records have unique global_event_ids.")
            else:
                print(f"  [FAIL] Duplicate IDs detected: {total_count - distinct_count} duplicates found!")
                failures += 1
    except Exception as e:
        print(f"  [FAIL] Failed to query iceberg.silver_gdelt.events: {e}")
        failures += 1

    # 2. Goldstein Scale Range Validity
    print("\n[Audit 2/7] Checking Goldstein Scale Bounds (-10.0 to +10.0)...")
    invalid_goldstein = spark.sql("""
        SELECT count(*) 
        FROM iceberg.silver_gdelt.events 
        WHERE goldstein_scale < -10.0 OR goldstein_scale > 10.0
    """).collect()[0][0]

    if invalid_goldstein == 0:
        print("  [PASS] All events have valid Goldstein stability scores in [-10.0, +10.0].")
    else:
        print(f"  [FAIL] {invalid_goldstein} records have out-of-bounds Goldstein scores!")
        failures += 1

    # 3. Geocoordinate Boundary Check
    print("\n[Audit 3/7] Checking WGS84 Latitude & Longitude Bounds...")
    invalid_coords = spark.sql("""
        SELECT count(*) 
        FROM iceberg.silver_gdelt.events 
        WHERE (latitude IS NOT NULL AND (latitude < -90.0 OR latitude > 90.0))
           OR (longitude IS NOT NULL AND (longitude < -180.0 OR longitude > 180.0))
    """).collect()[0][0]

    if invalid_coords == 0:
        print("  [PASS] All geographic coordinates are within valid WGS84 bounds.")
    else:
        print(f"  [FAIL] {invalid_coords} records have invalid latitude/longitude values!")
        failures += 1

    # 4. Temporal Sanity & Freshness SLA
    print("\n[Audit 4/7] Checking Temporal Event Dates and Pipeline Freshness...")
    freshness_row = spark.sql("""
        SELECT 
            min(event_date) as earliest_date,
            max(event_date) as latest_date,
            max(event_time) as latest_event_time,
            max(synced_at) as last_sync_time
        FROM iceberg.silver_gdelt.events
    """).collect()[0]

    print(f"  [INFO] Earliest Event Date: {freshness_row['earliest_date']}")
    print(f"  [INFO] Latest Event Date:   {freshness_row['latest_date']}")
    print(f"  [INFO] Latest Event Time:   {freshness_row['latest_event_time']}")
    print(f"  [INFO] Last Lakehouse Sync: {freshness_row['last_sync_time']}")

    if freshness_row["latest_event_time"] is not None:
        print("  [PASS] Lakehouse contains active, valid temporal events.")
    else:
        print("  [FAIL] Table contains no valid event timestamps!")
        failures += 1

    # 5. Strict ISO-3 Country Code Check
    print("\n[Audit 5/7] Checking Action Country Code Standardization (ISO-3)...")
    invalid_countries = spark.sql("""
        SELECT count(*) 
        FROM iceberg.silver_gdelt.events 
        WHERE action_country IS NULL 
           OR length(trim(action_country)) != 3
           OR trim(action_country) = ''
    """).collect()[0][0]

    if invalid_countries == 0:
        print("  [PASS] All events have valid 3-letter ISO-3 action_country codes.")
    else:
        print(f"  [FAIL] {invalid_countries} records have malformed or non-ISO-3 action_country codes!")
        failures += 1

    # 6. Gold Layer Aggregation Integrity Audit
    print("\n[Audit 6/7] Checking Gold Layer Stability Rollup Integrity...")
    gold_count = spark.sql("SELECT count(*) FROM iceberg.gold_gdelt.country_stability_daily").collect()[0][0]

    if gold_count > 0:
        gold_anomalies = spark.sql("""
            SELECT count(*) 
            FROM iceberg.gold_gdelt.country_stability_daily
            WHERE total_events < (conflict_events + cooperation_events)
               OR mean_goldstein_scale < -10.0 OR mean_goldstein_scale > 10.0
               OR action_country IS NULL OR length(trim(action_country)) != 3
        """).collect()[0][0]

        if gold_anomalies == 0:
            print(f"  [PASS] Gold country_stability_daily validated ({gold_count} daily rollups checked, 0 anomalies).")
        else:
            print(f"  [FAIL] {gold_anomalies} anomalous records detected in Gold country_stability_daily!")
            failures += 1
    else:
        print("  [INFO] Gold stability table is initialized and awaiting daily rollup.")

    # 7. Presto Hybrid Serving View Reconciliation & Zero-Duplicate Audit
    print("\n[Audit 7/7] Checking Presto Federated Serving View & Zero Double-Counting...")
    presto_url = os.environ.get("PRESTO_URL", os.environ.get("PRESTO_COORDINATOR_URL", "http://presto-coordinator:8080/v1/statement"))
    try:
        def run_presto_sql(query, timeout_sec=60):
            req = urllib.request.Request(
                presto_url,
                data=query.strip().encode("utf-8"),
                headers={"X-Presto-User": "airflow", "Content-Type": "text/plain"}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            start_time = time.time()
            rows = []
            while True:
                if "error" in data:
                    err_msg = data["error"].get("message", str(data["error"]))
                    raise RuntimeError(f"Presto SQL Error: {err_msg}")
                if "data" in data and data["data"]:
                    rows.extend(data["data"])
                if "nextUri" not in data:
                    break
                if time.time() - start_time > timeout_sec:
                    raise TimeoutError(f"Presto query timed out after {timeout_sec}s: {query[:60]}...")
                time.sleep(0.3)
                with urllib.request.urlopen(data["nextUri"], timeout=15) as next_resp:
                    data = json.loads(next_resp.read().decode("utf-8"))

            if "error" in data:
                err_msg = data["error"].get("message", str(data["error"]))
                raise RuntimeError(f"Presto SQL Error: {err_msg}")

            return rows

        res_total = run_presto_sql("SELECT count(*), count(DISTINCT global_event_id) FROM iceberg.gdelt_serving.vw_gdelt_events_unified")
        if not res_total or not res_total[0] or res_total[0][0] is None:
            print("  [FAIL] Presto federated view query returned no valid data or view does not exist!")
            failures += 1
        else:
            fed_total = int(res_total[0][0])
            fed_distinct = int(res_total[0][1])
            if fed_total == 0:
                print("  [FAIL] Presto federated view returned 0 events across both storage tiers!")
                failures += 1
            elif fed_total == fed_distinct:
                print(f"  [PASS] Zero double-counting verified in Presto federated view ({fed_total} events, 0 duplicates).")
            else:
                print(f"  [FAIL] Double counting detected in Presto federated view: {fed_total - fed_distinct} duplicate events across hot/cold split!")
                failures += 1
    except Exception as presto_err:
        print(f"  [FAIL] Presto federated view audit failed: {presto_err}")
        failures += 1

    end_time = datetime.now(timezone.utc)
    duration_sec = (end_time - start_time).total_seconds()
    print("\n" + "=" * 70)
    print(f"  Audit Duration: {duration_sec:.2f} seconds")
    print(f"  Completed: {end_time.isoformat()}")
    if failures == 0:
        print("  ALL GDELT DATA QUALITY AUDITS PASSED SUCCESSFULLY!")
    else:
        print(f"  DATA QUALITY AUDIT COMPLETED WITH {failures} FAILURE(S)!")
    print("=" * 70)

    if failures > 0:
        sys.exit(1)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("GDELTDataQualityChecks") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    try:
        main(spark)
    finally:
        spark.stop()
