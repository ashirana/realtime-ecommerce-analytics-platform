import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    sum,
    to_timestamp,
    coalesce,
    lit
)

from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    DoubleType
)

# ==========================================
# LOGGING CONFIGURATION
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = SparkSession.builder \
    .appName("RealTimeEcommerceAnalytics") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .config(
        "spark.jars",
        "../jars/postgresql-42.7.3.jar"
    ) \
    .getOrCreate()

# Reduce unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# Reduce local shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "8")

# ==========================================
# DEFINE EVENT SCHEMA
# ==========================================

schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("product_id", IntegerType()) \
    .add("category", StringType()) \
    .add("price", DoubleType()) \
    .add("device", StringType()) \
    .add("country", StringType()) \
    .add("city", StringType()) \
    .add("timestamp", StringType())

# ==========================================
# READ STREAM FROM KAFKA
# ==========================================

logger.info("Reading stream from Kafka...")

raw_df = spark.readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    ) \
    .option(
        "subscribe",
        "user_events"
    ) \
    .option(
        "startingOffsets",
        "earliest"
    ) \
    .load()

# ==========================================
# CONVERT KAFKA BYTES TO STRING
# ==========================================

json_df = raw_df.selectExpr(
    "CAST(value AS STRING)"
)

# ==========================================
# PARSE JSON EVENTS
# ==========================================

parsed_df = json_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")

# ==========================================
# CONVERT TIMESTAMP COLUMN
# ==========================================

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# ==========================================
# WINDOWED REAL-TIME AGGREGATION
# ==========================================

analytics_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("event_type")
    ).agg(

        count("*").alias("event_count"),

        coalesce(
            sum("price"),
            lit(0)
        ).alias("total_revenue")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("event_count"),
        col("total_revenue")
    )
# ==========================================
# WRITE TO POSTGRESQL
# ==========================================

def write_to_postgres(batch_df, batch_id):

    logger.info(
        f"Writing batch {batch_id} to PostgreSQL..."
    )

    batch_df.write \
        .format("jdbc") \
        .option(
            "url",
            "jdbc:postgresql://localhost:5432/ecommerce"
        ) \
        .option(
            "dbtable",
            "realtime_event_metrics"
        ) \
        .option(
            "user",
            "admin"
        ) \
        .option(
            "password",
            "admin"
        ) \
        .option(
            "driver",
            "org.postgresql.Driver"
        ) \
        .mode("append") \
        .save()

# ==========================================
# CONSOLE STREAM OUTPUT
# ==========================================

console_query = analytics_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# ==========================================
# POSTGRESQL STREAM OUTPUT
# ==========================================

postgres_query = analytics_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .start()

# ==========================================
# KEEP STREAM RUNNING
# ==========================================

logger.info("Spark streaming started...")

spark.streams.awaitAnyTermination()