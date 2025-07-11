import boto3
import json
import base64
import os
from decimal import Decimal
from botocore.exceptions import ClientError

# Initialize AWS resources
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('trips')

sns = boto3.client('sns')
SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-1:829115578678:trip-event-processing-failures'

def publish_error_notification(message):
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Trip Event Processing Error",
            Message=message
        )
    except Exception as sns_error:
        print(f"Failed to publish to SNS: {str(sns_error)}")

def convert_floats_to_decimal(obj):
    """Recursively convert all floats to Decimal for DynamoDB compatibility"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode and parse the Kinesis record
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            event_data = json.loads(payload)
            print(f"Received event: {event_data}")

            trip_id = event_data['trip_id']
            event_type = event_data['event_type']

            # Get existing trip from DynamoDB (if any)
            response = table.get_item(Key={'trip_id': trip_id})
            existing_item = response.get('Item', {})

            # Merge incoming data with existing item
            updated_item = {**existing_item, **event_data}

            # If both timestamps exist, mark trip as completed
            if 'pickup_datetime' in updated_item and 'dropoff_datetime' in updated_item:
                updated_item['status'] = 'completed'

            # Convert float fields to Decimal
            cleaned_item = convert_floats_to_decimal(updated_item)

            # Write back to DynamoDB
            table.put_item(Item=cleaned_item)
            print(f"Upserted trip_id {trip_id} into DynamoDB")

        except Exception as e:
            error_message = f"Error processing record: {str(e)}\nRaw record: {record}"
            print(error_message)
            publish_error_notification(error_message)
