import boto3
import pandas as pd
import json
import time
import random
import hashlib
from jsonschema import validate, ValidationError
from datetime import datetime
import os
import traceback

# Constants
STREAM_NAME = "trip_events_stream"
REGION = "eu-west-1"
SCHEMA_PATH = "schemas/trip_event_schema.json"
BATCH_SIZE_RANGE = (10, 100)
LOG_FILE = "simulation_log.jsonl"
LOG_S3_BUCKET = "trip-analytics-kpi-bucket"
LOG_S3_PREFIX = "simulation_logs/"

# AWS Clients
kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3")

# Load schema
with open(SCHEMA_PATH) as f:
    EVENT_SCHEMA = json.load(f)

seen_event_ids = set()
log_lines = []

def generate_event_id(row):
    return hashlib.md5(f"{row['trip_id']}-{row['event_type']}".encode()).hexdigest()

def prepare_events():
    df_start = pd.read_csv("data/trip_start.csv")
    df_start["event_type"] = "trip_start"
    df_end = pd.read_csv("data/trip_end.csv")
    df_end["event_type"] = "trip_end"
    all_events = pd.concat([df_start, df_end], ignore_index=True)
    all_events = all_events.sample(frac=1).reset_index(drop=True)
    return all_events

def batch_events(events_df):
    """Yield batches of random size between BATCH_SIZE_RANGE"""
    i = 0
    while i < len(events_df):
        batch_size = random.randint(*BATCH_SIZE_RANGE)
        yield events_df.iloc[i:i + batch_size]
        i += batch_size

def validate_event(event):
    try:
        validate(instance=event, schema=EVENT_SCHEMA)
        return True
    except ValidationError as e:
        log_lines.append(json.dumps({
            "level": "WARNING",
            "timestamp": datetime.utcnow().isoformat(),
            "message": f"Schema validation failed: {e.message}",
            "event": event
        }))
        return False

def send_to_kinesis(batch):
    records = []
    sent = 0

    for _, row in batch.iterrows():
        try:
            row_dict = row.dropna().to_dict()
            event_id = generate_event_id(row_dict)

            if event_id in seen_event_ids:
                continue
            if not validate_event(row_dict):
                continue

            seen_event_ids.add(event_id)
            records.append({
                'Data': json.dumps(row_dict),
                'PartitionKey': str(row_dict['trip_id'])
            })
        except Exception as e:
            log_lines.append(json.dumps({
                "level": "ERROR",
                "timestamp": datetime.utcnow().isoformat(),
                "message": str(e),
                "trace": traceback.format_exc()
            }))

    if records:
        try:
            response = kinesis.put_records(Records=records, StreamName=STREAM_NAME)
            failed = response['FailedRecordCount']
            sent = len(records) - failed
            print(f" Sent {sent} records to {STREAM_NAME}. Failed: {failed}")
            log_lines.append(json.dumps({
                "level": "INFO",
                "timestamp": datetime.utcnow().isoformat(),
                "sent": sent,
                "failed": failed
            }))
        except Exception as e:
            log_lines.append(json.dumps({
                "level": "ERROR",
                "timestamp": datetime.utcnow().isoformat(),
                "message": f"Failed to send to Kinesis: {str(e)}",
                "trace": traceback.format_exc()
            }))

def upload_log_to_s3():
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    s3_key = f"{LOG_S3_PREFIX}{timestamp}.jsonl"

    with open(LOG_FILE, "w") as f:
        for line in log_lines:
            f.write(line + "\n")

    s3.upload_file(LOG_FILE, LOG_S3_BUCKET, s3_key)
    print(f" Uploaded log to s3://{LOG_S3_BUCKET}/{s3_key}")
    os.remove(LOG_FILE)

def main():
    events = prepare_events()
    for batch in batch_events(events):
        send_to_kinesis(batch)
        time.sleep(1.5)
    upload_log_to_s3()

if __name__ == "__main__":
    main()
