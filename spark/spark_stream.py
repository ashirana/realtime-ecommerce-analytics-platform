import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    sum
)
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    DoubleType
)


# ==============================
# LOGGING
# ==============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ==============================
# CREATE SPARK SESSION
# ==============================

spark = SparkSession.builder \
    .appName("RealTimeEcommerceAnalytics") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ==============================
# DEFINE EVENT SCHEMA
# ==============================

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


# ==============================
# READ STREAM FROM KAFKA
# ==============================

logger.info("Reading stream from Kafka...")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()


# ==============================
# CONVERT KAFKA BYTES TO STRING
# ==============================

json_df = raw_df.selectExpr(
    "CAST(value AS STRING)"
)


# ==============================
# PARSE JSON EVENTS
# ==============================

parsed_df = json_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")


# ==============================
# REAL-TIME AGGREGATION
# ==============================

analytics_df = parsed_df.groupBy(
    "event_type"
).agg(
    count("*").alias("event_count"),
    sum("price").alias("total_revenue")
)


# ==============================
# OUTPUT STREAM
# ==============================

query = analytics_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()


# ==============================
# KEEP STREAM RUNNING
# ==============================

logger.info("Spark streaming started...")

query.awaitTermination()