from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Trending Products") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr(
    "CAST(value AS STRING)"
)

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True)
])

parsed_df = json_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")

df = parsed_df.withColumn(
    "event_time",
    to_timestamp(
        col("event_time"),
        "yyyy-MM-dd HH:mm:ss 'UTC'"
    )
)

df = df.filter(
    col("event_type") == "view"
)

df = df.withWatermark(
    "event_time",
    "10 minutes"
)

result = df.groupBy(
    window(
        col("event_time"),
        "1 hour"
    ),
    col("product_id")
).count()

top_products = result.orderBy(
    col("count").desc()
)

query = top_products.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
