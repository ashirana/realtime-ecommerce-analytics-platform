from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MinIOTest") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ])
    ) \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.access.key","admin")
hadoop_conf.set("fs.s3a.secret.key","password123")
hadoop_conf.set("fs.s3a.endpoint","http://localhost:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
df = spark.createDataFrame(
    [
        (1, "hello"),
        (2, "world")
    ],
    ["id", "value"]
)

df.write.mode("overwrite").parquet(
    "s3a://ecommerce-data-lake/test/"
)

print("SUCCESS")

spark.stop()
