import boto3
import pandas as pd
import json
import time
import random
import hashlib
from jsonschema import validate, ValidationError

# Constants
STREAM_NAME = "trip_events_stream"
REGION = "eu-west-1"
SCHEMA_PATH = "schemas/trip_event_schema.json"
BATCH_SIZE = 100

# Kinesis client
kinesis = boto3.client("kinesis", region_name=REGION)

# Load schema once
with open(SCHEMA_PATH) as f:
    EVENT_SCHEMA = json.load(f)

# Deduplication tracker
seen_event_ids = set()

def generate_event_id(row):
    """Unique ID based on trip_id + event_type"""
    return hashlib.md5(f"{row['trip_id']}-{row['event_type']}".encode()).hexdigest()

def prepare_events():
    """Load, label, and combine events from CSVs"""
    df_start = pd.read_csv("data/trip_start.csv")
    df_start["event_type"] = "trip_start"

    df_end = pd.read_csv("data/trip_end.csv")
    df_end["event_type"] = "trip_end"

    all_events = pd.concat([df_start, df_end], ignore_index=True)
    all_events = all_events.sample(frac=1).reset_index(drop=True)  # Shuffle
    return all_events

def batch_events(events_df):
    """Yield batches of events"""
    for i in range(0, len(events_df), BATCH_SIZE):
        yield events_df.iloc[i:i + BATCH_SIZE]

def validate_event(event):
    """Validate against JSON schema"""
    try:
        validate(instance=event, schema=EVENT_SCHEMA)
        return True
    except ValidationError as e:
        print(f" Invalid event: {e.message}")
        return False

def send_to_kinesis(batch):
    """Send one batch of events to Kinesis"""
    records = []

    for _, row in batch.iterrows():
        row_dict = row.dropna().to_dict()
        event_id = generate_event_id(row_dict)

        # Deduplication
        if event_id in seen_event_ids:
            continue

        # Validation
        if not validate_event(row_dict):
            continue

        seen_event_ids.add(event_id)
        records.append({
            'Data': json.dumps(row_dict),
            'PartitionKey': str(row_dict['trip_id'])
        })

    # Push to Kinesis
    if records:
        response = kinesis.put_records(Records=records, StreamName=STREAM_NAME)
        print(f" Sent {len(records)} records to {STREAM_NAME}. Failed: {response['FailedRecordCount']}")

def main():
    events = prepare_events()
    for batch in batch_events(events):
        send_to_kinesis(batch)
        time.sleep(1.5)  # Delay to simulate real flow

if __name__ == "__main__":
    main()
