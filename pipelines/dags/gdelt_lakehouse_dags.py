"""
Airflow DAG Suite for GDELT Global Geopolitical Lakehouse
Orchestrates direct batch backfills to Iceberg, 5-minute incremental Lakehouse syncs,
daily Gold aggregations, Data Quality checks, hot tier purges, and Iceberg compaction.
"""

import os
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone
import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "lakehouse_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/?replicaSet=rs0")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE", "market_data")
SPARK_SUBMIT = f"/opt/spark/bin/spark-submit --master {SPARK_MASTER} --deploy-mode client"

# ---------------------------------------------------------------------------
# DAG 1: Historical GDELT Archive Backfill Pipeline (Manual / On-Demand)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="backfill_gdelt_archives",
    default_args=default_args,
    description="Downloads daily GDELT archives and bulk-loads directly into Iceberg Silver",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "gdelt", "lakehouse", "backfill"],
) as dag_backfill:

    task_download = BashOperator(
        task_id="download_and_stage_archives",
        bash_command="python /opt/airflow/ingestion/batch/download_archives.py",
    )

    task_spark_load = BashOperator(
        task_id="spark_load_archives_to_iceberg",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/jobs/load_archives_to_iceberg.py",
    )

    task_deploy_views = BashOperator(
        task_id="deploy_presto_serving_views",
        bash_command="python /opt/airflow/jobs/deploy_presto_views.py",
    )

    task_trigger_sync = TriggerDagRunOperator(
        task_id="trigger_lakehouse_sync",
        trigger_dag_id="sync_mongo_to_iceberg",
        wait_for_completion=False,
    )

    task_download >> task_spark_load >> task_deploy_views >> task_trigger_sync

# ---------------------------------------------------------------------------
# DAG 2: Incremental MongoDB -> Iceberg Lakehouse Sync (Every 5 Minutes)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="sync_mongo_to_iceberg",
    default_args=default_args,
    description="Micro-batch sync reading Mongo _modifiedTS cursor and executing Iceberg MERGE INTO",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["sync", "iceberg", "streaming", "gdelt"],
) as dag_sync:

    task_sync = BashOperator(
        task_id="spark_merge_mongo_to_iceberg",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/jobs/sync_mongo_to_iceberg.py",
    )

    task_sync_views = BashOperator(
        task_id="ensure_presto_serving_views",
        bash_command="python /opt/airflow/jobs/deploy_presto_views.py",
    )

    task_sync >> task_sync_views

# ---------------------------------------------------------------------------
# DAG 3: Daily Gold Layer Rebuild (Daily at 01:00 UTC)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="build_gold_daily",
    default_args=default_args,
    description="Computes daily country stability scores and bilateral conflict matrices",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "analytics", "geopolitics"],
) as dag_gold:

    task_pre_gold_dq = BashOperator(
        task_id="spark_data_quality_gate",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/jobs/dq_checks.py",
    )

    task_build_gold = BashOperator(
        task_id="spark_build_gold_tables",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/jobs/build_gold.py",
    )

    task_pre_gold_dq >> task_build_gold

# ---------------------------------------------------------------------------
# DAG 4: Data Quality & Reconciliation Audits (Hourly)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="data_quality_audits",
    default_args=default_args,
    description="Runs primary key uniqueness, coordinate bounds, and freshness audits",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dq", "quality", "audit"],
) as dag_dq:

    task_dq = BashOperator(
        task_id="spark_data_quality_checks",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/jobs/dq_checks.py",
    )

# ---------------------------------------------------------------------------
# DAG 5: Safe Hot-Tier Purge Policy (Every 30 Minutes)
# ---------------------------------------------------------------------------
def get_lakehouse_sync_watermark():
    """
    Queries Presto to fetch the latest committed sync watermark from iceberg.silver_gdelt.sync_state.
    Returns (cursor_datetime, max_event_id) or (None, None) if not available.
    """
    presto_url = os.environ.get("PRESTO_COORDINATOR_URL", "http://presto-coordinator:8080/v1/statement")
    query = """
        SELECT 
            COALESCE(CAST(max_by(cursor_modified_ts, synced_at) AS VARCHAR), '1970-01-01 00:00:00') as last_cursor,
            COALESCE(MAX(max_event_id), 0) as max_id
        FROM iceberg.silver_gdelt.sync_state
        WHERE table_name = 'mongo_sync'
    """
    req = urllib.request.Request(
        presto_url,
        data=query.encode("utf-8"),
        headers={"X-Presto-User": "airflow", "Content-Type": "text/plain"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        for _ in range(30):
            if "data" in data and data["data"]:
                row = data["data"][0]
                cursor_str, max_id = str(row[0]), int(row[1])
                try:
                    clean_str = cursor_str.replace(" UTC", "").replace(" ", "T").rstrip("Z")
                    cursor_dt = datetime.fromisoformat(clean_str).replace(tzinfo=timezone.utc)
                except Exception:
                    cursor_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
                return cursor_dt, max_id
            if "nextUri" not in data:
                break
            time.sleep(0.5)
            with urllib.request.urlopen(data["nextUri"], timeout=15) as next_resp:
                data = json.loads(next_resp.read().decode("utf-8"))
    return None, None

def purge_confirmed_records():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=48)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)

    try:
        cursor_dt, max_id = get_lakehouse_sync_watermark()
    except Exception as e:
        print(f"[!] Warning: Failed to query sync watermark from Presto: {e}. Aborting purge to protect un-synced data.")
        client.close()
        return

    if not cursor_dt or cursor_dt.year <= 1970 or not max_id or max_id <= 0:
        print("[!] Warning: Lakehouse sync watermark not yet established in Iceberg sync_state. Skipping purge to guarantee zero data loss.")
        client.close()
        return

    cursor_ms = int(cursor_dt.timestamp() * 1000)
    print(f"[*] Verified sync watermark: cursor={cursor_dt}, max_id={max_id}. Purging confirmed synced records older than 48h...")

    # Strict 3-criteria purge filter:
    # 1. Event age > 48 hours
    # 2. Confirmed committed to Iceberg (global_event_id <= max_id)
    # 3. Modified prior to committed sync watermark (_modifiedTS <= cursor)
    delete_filter = {
        "$and": [
            {
                "$or": [
                    {"event_time": {"$lt": cutoff_time}},
                    {"event_time": {"$lt": cutoff_ms}}
                ]
            },
            {"global_event_id": {"$lte": max_id}},
            {
                "$or": [
                    {"_modifiedTS": {"$lte": cursor_dt}},
                    {"_modifiedTS": {"$lte": cursor_ms}}
                ]
            }
        ]
    }

    result = db.events.delete_many(delete_filter)
    print(f"[+] Safely purged {result.deleted_count} confirmed-synced events from MongoDB ({MONGO_DATABASE}.events).")
    client.close()

with DAG(
    dag_id="purge_hot_tier",
    default_args=default_args,
    description="Purges confirmed synced events older than 48 hours to cap MongoDB disk",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance", "mongodb", "purge"],
) as dag_purge:

    task_purge = PythonOperator(
        task_id="purge_mongodb_hot_tier",
        python_callable=purge_confirmed_records,
    )

# ---------------------------------------------------------------------------
# DAG 6: Lakehouse Compaction & Snapshot Cleanup (Daily 02:00 UTC)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="lakehouse_maintenance",
    default_args=default_args,
    description="Runs Iceberg small-file binpack compaction and snapshot expiration",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["iceberg", "compaction", "maintenance"],
) as dag_maint:

    task_maintenance = BashOperator(
        task_id="spark_iceberg_maintenance",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/jobs/maintenance.py",
    )
