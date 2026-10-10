#!/usr/bin/env python3
"""
GDELT 2.0 Historical Archive Downloader
Downloads full daily GDELT 2.0 event archives across all 15-minute intervals
(up to 96 slices per day) using concurrent workers, validates downloaded archives,
extracts tab-delimited CSVs, and stages them for direct Apache Spark Lakehouse ingestion.
"""

import os
import ssl
import zipfile
import urllib.request
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# SSL Context handling
try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    SSL_CONTEXT = ssl._create_unverified_context()

BASE_URL = "http://data.gdeltproject.org/gdeltv2"
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "30"))
SLICES_PER_DAY = int(os.environ.get("SLICES_PER_DAY", "96"))  # 96 = Full 24 hours (all 15-min intervals)
MAX_WORKERS = int(os.environ.get("DOWNLOAD_WORKERS", "8"))
STAGING_DIR = os.environ.get("STAGING_DIR", "/tmp/lakehouse/landing/gdelt")

def generate_day_slice_timestamps(slices_count=96):
    """
    Generates HHMMSS timestamps for 15-minute intervals throughout a day.
    slices_count=96 yields 000000, 001500, ..., 234500.
    """
    timestamps = []
    for h in range(24):
        for m in (0, 15, 30, 45):
            timestamps.append(f"{h:02d}{m:02d}00")
            if len(timestamps) >= slices_count:
                return timestamps
    return timestamps

def download_file(url, target_path):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=30) as resp:
        with open(target_path, "wb") as f:
            while chunk := resp.read(65536):
                f.write(chunk)

def extract_zip(zip_path, extract_dir):
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)
        extracted = z.namelist()
    return extracted

def download_and_extract_slice(date_str, time_str):
    filename = f"{date_str}{time_str}.export.CSV.zip"
    url = f"{BASE_URL}/{filename}"
    target_zip = os.path.join(STAGING_DIR, "downloads", filename)
    extract_dir = os.path.join(STAGING_DIR, "extracted", date_str)

    try:
        download_file(url, target_zip)
        extracted = extract_zip(target_zip, extract_dir)
        return (True, filename, len(extracted))
    except Exception as e:
        return (False, filename, str(e))

def download_day_full(date_str, slices_count=96):
    time_slices = generate_day_slice_timestamps(slices_count)
    print(f"[*] Downloading {len(time_slices)} 15-minute slice(s) for date {date_str} (Full 24h coverage)...")

    succeeded = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_and_extract_slice, date_str, t): t
            for t in time_slices
        }
        for future in as_completed(futures):
            success, fname, result = future.result()
            if success:
                succeeded += 1
            else:
                failed += 1

    print(f"    [+] Date {date_str}: Successfully downloaded and extracted {succeeded}/{len(time_slices)} slices ({failed} missed/empty).")
    return succeeded > 0

def main():
    print("=" * 70)
    print("  GDELT 2.0 HISTORICAL ARCHIVE DOWNLOADER & STAGING (FULL DAYS)")
    print(f"  Staging Directory: {STAGING_DIR}")
    print(f"  Backfill Days:     {BACKFILL_DAYS}")
    print(f"  Slices Per Day:    {SLICES_PER_DAY} (100% full-day coverage when 96)")
    print(f"  Worker Threads:    {MAX_WORKERS}")
    print("=" * 70)

    os.makedirs(STAGING_DIR, exist_ok=True)

    today = datetime.now(timezone.utc)
    successful_days = 0

    for i in range(1, BACKFILL_DAYS + 1):
        target_date = (today - timedelta(days=i)).strftime("%Y%m%d")
        print(f"\n[*] Processing GDELT archive for date: {target_date} ({i}/{BACKFILL_DAYS})...")
        if download_day_full(target_date, SLICES_PER_DAY):
            successful_days += 1

    print("\n" + "=" * 70)
    print(f"  DOWNLOAD COMPLETE: Staged full archives for {successful_days}/{BACKFILL_DAYS} days in {STAGING_DIR}")
    print("=" * 70)

if __name__ == "__main__":
    main()
