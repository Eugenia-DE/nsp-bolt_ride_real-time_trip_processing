import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, sum, avg, count, max, min
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Read job name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from DynamoDB
dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "trips",
        "dynamodb.throughput.read.percent": "1.0",
        "dynamodb.splits": "100"
    }
)

# Convert to DataFrame
df = dyf.toDF()

# Filter completed trips
df = df.filter(col("status") == "completed")

# Ensure dropoff_datetime is in date format
df = df.withColumn("trip_date", to_date(col("dropoff_datetime")))

# Aggregate KPIs per day
agg_df = df.groupBy("trip_date").agg(
    count("*").alias("count_trips"),
    sum("fare_amount").alias("total_fare"),
    avg("fare_amount").alias("average_fare"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare")
)

# Write to S3 as partitioned JSON
agg_df.write.mode("overwrite").partitionBy("trip_date").json("s3://trip-analytics-kpi-bucket/kpis/")

job.commit()
