import boto3
import json
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

TABLE_NAME = "trips"
BUCKET_NAME = "trip-analytics-kpi-bucket"
OUTPUT_PREFIX = "kpis/"

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    kpis_by_date = defaultdict(list)

    # DynamoDB Scan
    response = table.scan()
    items = response.get('Items', [])

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    # Filter completed and group by dropoff date
    for item in items:
        if item.get("status") != "completed":
            continue

        dropoff = item.get("dropoff_datetime")
        fare = item.get("fare_amount")

        if not dropoff or fare is None:
            continue

        try:
            date = dropoff.split(" ")[0]  # extract YYYY-MM-DD
            kpis_by_date[date].append(float(fare))
        except Exception as e:
            print(f"Skipping bad record: {item} -- {e}")

    # Compute and upload KPIs
    for date, fares in kpis_by_date.items():
        kpi = {
            "date": date,
            "count_trips": len(fares),
            "total_fare": round(sum(fares), 2),
            "average_fare": round(sum(fares) / len(fares), 2),
            "max_fare": round(max(fares), 2),
            "min_fare": round(min(fares), 2)
        }

        key = f"{OUTPUT_PREFIX}{date}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(kpi),
            ContentType="application/json"
        )
        print(f" Wrote KPIs to s3://{BUCKET_NAME}/{key}")

    return {"statusCode": 200, "message": "KPI aggregation complete."}
