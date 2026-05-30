from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgresTest") \
    .master("local[*]") \
    .config(
        "spark.jars",
        "../jars/postgresql-42.7.3.jar"
    ) \
    .getOrCreate()

df = spark.createDataFrame(
    [(1, "hello")],
    ["id", "value"]
)

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ecommerce") \
    .option("dbtable", "test_table") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("SUCCESS")
spark.stop()