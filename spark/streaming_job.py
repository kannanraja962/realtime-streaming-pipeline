from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformations import apply_transformations
from utils.logger import setup_logger

logger = setup_logger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeStreamingPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming events
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("product_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("value", DoubleType())
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and apply schema
parsed_df = df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Apply transformations
transformed_df = apply_transformations(parsed_df)

# Windowed Aggregation (5-minute tumbling window)
windowed_aggregations = transformed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ) \
    .agg(
        count("*").alias("event_count"),
        avg("value").alias("avg_value"),
        sum("value").alias("total_value")
    )

# Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/streaming_db") \
        .option("dbtable", "aggregated_events") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()
    logger.info(f"Batch {batch_id} written to PostgreSQL")

# Start streaming query
query = windowed_aggregations \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

logger.info("Streaming job started. Waiting for data...")
query.awaitTermination()
