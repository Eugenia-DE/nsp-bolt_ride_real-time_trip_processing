import pytest
import pandas as pd
import json
from unittest.mock import patch, MagicMock
from src.ingestion.simulate_stream import (
    validate_event,
    generate_event_id,
    send_to_kinesis
)

# Load schema for sample testing
with open("schemas/trip_event_schema.json") as f:
    EVENT_SCHEMA = json.load(f)

@pytest.fixture
def valid_trip_start_event():
    return {
        "trip_id": "T123",
        "pickup_location_id": 1,
        "dropoff_location_id": 2,
        "vendor_id": 1,
        "pickup_datetime": "2025-07-10T10:00:00",
        "estimated_dropoff_datetime": "2025-07-10T10:30:00",
        "estimated_fare_amount": 25.50,
        "event_type": "trip_start"
    }

@pytest.fixture
def valid_trip_end_event():
    return {
        "trip_id": "T123",
        "dropoff_datetime": "2025-07-10T10:35:00",
        "rate_code": 1,
        "passenger_count": 2,
        "trip_distance": 5.0,
        "fare_amount": 27.75,
        "tip_amount": 3.00,
        "payment_type": 1,
        "trip_type": 1,
        "event_type": "trip_end"
    }

def test_schema_validation(valid_trip_start_event):
    assert validate_event(valid_trip_start_event) is True

def test_invalid_event_missing_required_field():
    invalid_event = {
        "pickup_location_id": 1,
        "event_type": "trip_start"  # missing trip_id
    }
    assert validate_event(invalid_event) is False

def test_deduplication_key():
    event1 = {"trip_id": "ABC", "event_type": "trip_start"}
    event2 = {"trip_id": "ABC", "event_type": "trip_start"}
    assert generate_event_id(event1) == generate_event_id(event2)

@patch("src.ingestion.simulate_stream.kinesis.put_records")
def test_send_batch_deduplicates_and_sends(mock_put_records, valid_trip_start_event, valid_trip_end_event):
    # Create test batch with duplicates
    df = pd.DataFrame([valid_trip_start_event, valid_trip_start_event, valid_trip_end_event])
    
    # Reset seen_event_ids inside test scope
    from src.ingestion import simulate_stream
    simulate_stream.seen_event_ids = set()

    mock_put_records.return_value = {"FailedRecordCount": 0}

    send_to_kinesis(df)

    # Expect only 2 unique records sent to Kinesis
    assert mock_put_records.called
    call_args = mock_put_records.call_args[1]["Records"]
    assert len(call_args) == 2
