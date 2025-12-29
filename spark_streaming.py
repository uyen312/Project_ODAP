from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from exchange_rate import get_vnd_rate

# CONFIG
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "credit_card_transactions"

HDFS_BASE = "hdfs://localhost:9000/credit_card_transactions/"
OUTPUT_PATH = HDFS_BASE + "data/"
CHECKPOINT_PATH = HDFS_BASE + "checkpoint/"

# SPARK SESSION
spark = SparkSession.builder \
    .appName("CreditCardStreamingWithExchangeBackup") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# SCHEMA CHO JSON
schema = StructType([
    StructField("user", StringType()),
    StructField("card", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
    StructField("time", StringType()),
    StructField("amount", DoubleType()),
    StructField("use_chip", StringType()),
    StructField("merchant_name", StringType()),
    StructField("merchant_city", StringType()),
    StructField("merchant_state", StringType()),
    StructField("zip", StringType()),
    StructField("mcc", StringType()),
    StructField("errors", StringType()),
    StructField("is_fraud", StringType())
])


# ĐỌC DỮ LIỆU TỪ KAFKA
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "credit_card_transactions") \
    .option("startingOffsets", "latest") \
    .load()


# PARSE JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")


# LỌC GIAO DỊCH HỢP LỆ
valid_df = parsed_df.filter(col("is_fraud") == "No")


# CHUẨN HÓA DATETIME
valid_df = valid_df.withColumn(
    "transaction_datetime",
    expr("""
        try_to_timestamp(
            concat_ws(' ', concat_ws('-', year, month, day), concat(time, ':00')),
            'yyyy-M-d HH:mm:ss'
        )
    """)
)


# TÍNH AMOUNT_VND
vnd_rate = get_vnd_rate()
# print(f"Current USD->VND rate: {vnd_rate}")

valid_df = valid_df.withColumn("amount_vnd", col("amount") * lit(vnd_rate))


# OUTPUT STREAM (console)
query = valid_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()


# OUTPUT STREAM (HDFS PARQUET)
parquet_query = valid_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()
parquet_query.awaitTermination()