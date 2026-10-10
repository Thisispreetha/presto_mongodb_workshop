#!/usr/bin/env python3
"""
GDELT 2.0 Live Event Stream Producer
Polls GDELT 2.0 live 15-minute broadcast feed (http://data.gdeltproject.org/gdeltv2/lastupdate.txt),
downloads the latest export ZIP, parses and normalizes global events (FIPS -> ISO-3 country codes,
extracts DATEADDED UTC timestamps), preserves NULL for missing values, maintains durable cursor state,
automatically catches up on missed intervals across restarts, and publishes structured JSON events to Apache Kafka.
"""

import os
import sys
import io
import time
import json
import zipfile
import logging
import ssl
import urllib.request
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("gdelt_stream_producer")

# SSL Context handling
try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    SSL_CONTEXT = ssl._create_unverified_context()

try:
    from confluent_kafka import Producer
except ImportError:
    logger.error("'confluent-kafka' library is required. Install via: pip install confluent-kafka")
    sys.exit(1)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_EVENTS = os.environ.get("TOPIC_EVENTS", "gdelt.events.v1")
TOPIC_DLQ = os.environ.get("TOPIC_DLQ", "gdelt.dlq.v1")
GDELT_LASTUPDATE_URL = os.environ.get("GDELT_LASTUPDATE_URL", "http://data.gdeltproject.org/gdeltv2/lastupdate.txt")
POLL_INTERVAL_SEC = int(os.environ.get("GDELT_POLL_INTERVAL_SEC", "60"))
STATE_FILE = os.environ.get("PRODUCER_STATE_FILE", "/app/state/last_processed_file.txt")

# Comprehensive FIPS 10-4 to ISO-3 alpha-3 mapping for all 243 nations and territories
FIPS_TO_ISO3 = {
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

def normalize_country(fips_code, fallback_code=None):
    if fips_code:
        fips_clean = fips_code.strip().upper()
        if fips_clean in FIPS_TO_ISO3:
            return FIPS_TO_ISO3[fips_clean]
    if fallback_code:
        fb_clean = fallback_code.strip().upper()
        if len(fb_clean) == 3:
            return fb_clean
        if fb_clean in FIPS_TO_ISO3:
            return FIPS_TO_ISO3[fb_clean]
    return "WLD"

# Kafka Connect JsonConverter Schema for market_data.events
# All non-key metrics have optional: True so missing fields stay NULL without schema errors
EVENT_SCHEMA = {
    "type": "struct",
    "fields": [
        {"field": "_id", "type": "string"},
        {"field": "global_event_id", "type": "int64"},
        {"field": "event_date", "type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "version": 1},
        {"field": "event_time", "type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "version": 1},
        {"field": "actor1_name", "type": "string", "optional": True},
        {"field": "actor1_country", "type": "string", "optional": True},
        {"field": "actor2_name", "type": "string", "optional": True},
        {"field": "actor2_country", "type": "string", "optional": True},
        {"field": "event_code", "type": "string", "optional": True},
        {"field": "quad_class", "type": "int32", "optional": True},
        {"field": "goldstein_scale", "type": "double", "optional": True},
        {"field": "num_mentions", "type": "int32", "optional": True},
        {"field": "num_sources", "type": "int32", "optional": True},
        {"field": "num_articles", "type": "int32", "optional": True},
        {"field": "avg_tone", "type": "double", "optional": True},
        {"field": "action_country", "type": "string", "optional": True},
        {"field": "latitude", "type": "double", "optional": True},
        {"field": "longitude", "type": "double", "optional": True},
        {"field": "source_url", "type": "string", "optional": True},
        {"field": "source", "type": "string"},
        {"field": "received_at", "type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "version": 1}
    ]
}

def get_kafka_producer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "gdelt-stream-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5
    }
    return Producer(conf)

def delivery_callback(err, msg):
    if err:
        logger.error(f"Kafka message delivery failed: {err}")

def load_last_processed_url():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception as e:
            logger.warning(f"Could not read state file {STATE_FILE}: {e}")
    return None

def save_last_processed_url(url):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w") as f:
            f.write(url.strip())
    except Exception as e:
        logger.error(f"Failed to persist state file {STATE_FILE}: {e}")

def extract_timestamp_from_url(url):
    if not url:
        return None
    try:
        basename = os.path.basename(url)
        ts_str = basename[:14]
        return datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def get_latest_export_url():
    req = urllib.request.Request(GDELT_LASTUPDATE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=20) as resp:
        content = resp.read().decode("utf-8").strip()
    lines = content.splitlines()
    if not lines:
        return None
    export_line = lines[0]
    parts = export_line.split()
    if len(parts) >= 3:
        return parts[2]
    return None

def parse_and_publish_zip(producer, zip_url):
    logger.info(f"Downloading GDELT broadcast: {zip_url}...")
    req = urllib.request.Request(zip_url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=45) as resp:
        zip_bytes = resp.read()

    event_count = 0
    now_ms = int(time.time() * 1000)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for fname in z.namelist():
            with z.open(fname) as f:
                for line_bytes in f:
                    line = line_bytes.decode("utf-8", errors="ignore").strip()
                    if not line:
                        continue
                    cols = line.split("\t")
                    if len(cols) < 58:
                        continue
                    try:
                        gid = int(cols[0])
                        sql_date_str = cols[1]  # YYYYMMDD
                        event_date = datetime.strptime(sql_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
                        event_date_ms = int(event_date.timestamp() * 1000)

                        # Parse DATEADDED (col 59)
                        date_added_str = cols[59].strip() if len(cols) > 59 and cols[59] else None
                        if date_added_str and len(date_added_str) >= 14:
                            event_time = datetime.strptime(date_added_str[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                        else:
                            event_time = event_date
                        event_time_ms = int(event_time.timestamp() * 1000)

                        actor1_name = cols[6].strip() if len(cols) > 6 and cols[6].strip() else None
                        actor1_country = cols[7].strip() if len(cols) > 7 and cols[7].strip() else None
                        actor2_name = cols[16].strip() if len(cols) > 16 and cols[16].strip() else None
                        actor2_country = cols[17].strip() if len(cols) > 17 and cols[17].strip() else None

                        event_code = cols[26].strip() if len(cols) > 26 and cols[26].strip() else None

                        # Missing numeric and scale values remain None (JSON null) - no invented values
                        quad_class = int(cols[29]) if len(cols) > 29 and cols[29].strip() else None
                        goldstein = float(cols[30]) if len(cols) > 30 and cols[30].strip() else None
                        num_mentions = int(cols[31]) if len(cols) > 31 and cols[31].strip() else None
                        num_sources = int(cols[32]) if len(cols) > 32 and cols[32].strip() else None
                        num_articles = int(cols[33]) if len(cols) > 33 and cols[33].strip() else None
                        avg_tone = float(cols[34]) if len(cols) > 34 and cols[34].strip() else None

                        fips_country = cols[53].strip() if len(cols) > 53 and cols[53].strip() else None
                        action_country = normalize_country(fips_country, actor1_country)

                        # Coordinates stay None if missing - never invented 0.0 (Null Island)
                        lat = float(cols[56]) if len(cols) > 56 and cols[56].strip() else None
                        lon = float(cols[57]) if len(cols) > 57 and cols[57].strip() else None

                        source_url = cols[60].strip() if len(cols) > 60 and cols[60].strip() else (cols[-1].strip() if len(cols) > 58 and cols[-1].strip() else None)

                        payload = {
                            "_id": f"EVT_{gid}",
                            "global_event_id": gid,
                            "event_date": event_date_ms,
                            "event_time": event_time_ms,
                            "actor1_name": actor1_name,
                            "actor1_country": actor1_country,
                            "actor2_name": actor2_name,
                            "actor2_country": actor2_country,
                            "event_code": event_code,
                            "quad_class": quad_class,
                            "goldstein_scale": goldstein,
                            "num_mentions": num_mentions,
                            "num_sources": num_sources,
                            "num_articles": num_articles,
                            "avg_tone": avg_tone,
                            "action_country": action_country,
                            "latitude": lat,
                            "longitude": lon,
                            "source_url": source_url,
                            "source": "stream",
                            "received_at": now_ms
                        }

                        msg = {"schema": EVENT_SCHEMA, "payload": payload}
                        producer.produce(
                            TOPIC_EVENTS,
                            key=action_country.encode("utf-8"),
                            value=json.dumps(msg).encode("utf-8"),
                            callback=delivery_callback
                        )
                        event_count += 1

                        if event_count % 500 == 0:
                            producer.poll(0)

                    except Exception as err:
                        producer.produce(
                            TOPIC_DLQ,
                            value=json.dumps({"raw": line, "error": str(err), "time": now_ms}).encode("utf-8")
                        )

    producer.flush(15)
    logger.info(f"[+] Published {event_count} GDELT events to Kafka topic '{TOPIC_EVENTS}' from {zip_url}")
    return event_count

def catch_up_missed_slices(producer, last_url, current_url):
    last_dt = extract_timestamp_from_url(last_url)
    curr_dt = extract_timestamp_from_url(current_url)

    if not last_dt or not curr_dt or curr_dt <= last_dt:
        return

    gap_minutes = int((curr_dt - last_dt).total_seconds() / 60)
    if gap_minutes > 15:
        logger.info(f"[*] Detected gap of {gap_minutes} minutes between {last_dt} and {curr_dt}. Catching up on missed 15-minute slices...")
        step_dt = last_dt + timedelta(minutes=15)
        # Limit catchup to last 24 hours to prevent endless historical backfill
        max_lookback = curr_dt - timedelta(hours=24)
        if step_dt < max_lookback:
            step_dt = max_lookback

        while step_dt < curr_dt:
            slice_ts_str = step_dt.strftime("%Y%m%d%H%M%S")
            slice_url = f"http://data.gdeltproject.org/gdeltv2/{slice_ts_str}.export.CSV.zip"
            logger.info(f"    [+] Catching up missed slice: {slice_url}...")
            try:
                parse_and_publish_zip(producer, slice_url)
                save_last_processed_url(slice_url)
            except Exception as slice_err:
                logger.warning(f"    [!] Missed slice {slice_url} could not be downloaded/processed: {slice_err}")
            step_dt += timedelta(minutes=15)

def main():
    logger.info("=" * 70)
    logger.info("  GDELT 2.0 REAL-TIME GLOBAL EVENT STREAM PRODUCER")
    logger.info(f"  Kafka Broker:  {KAFKA_BROKER}")
    logger.info(f"  Target Topic:  {TOPIC_EVENTS}")
    logger.info(f"  Poll Interval: {POLL_INTERVAL_SEC}s")
    logger.info(f"  State File:    {STATE_FILE}")
    logger.info("=" * 70)

    producer = get_kafka_producer()
    last_processed_url = load_last_processed_url()
    if last_processed_url:
        logger.info(f"Loaded durable state from {STATE_FILE}: last processed = {last_processed_url}")

    try:
        while True:
            try:
                latest_url = get_latest_export_url()
                if latest_url and latest_url != last_processed_url:
                    logger.info(f"Detected new GDELT broadcast file: {latest_url}")
                    if last_processed_url:
                        catch_up_missed_slices(producer, last_processed_url, latest_url)
                    parse_and_publish_zip(producer, latest_url)
                    last_processed_url = latest_url
                    save_last_processed_url(latest_url)
                else:
                    logger.info("No new GDELT file published yet. Waiting...")

            except Exception as e:
                logger.error(f"Error checking GDELT broadcast: {e}. Retrying in {POLL_INTERVAL_SEC}s...")

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Stream producer stopping...")
    finally:
        producer.flush(10)
        logger.info("Stream producer stopped.")

if __name__ == "__main__":
    main()
