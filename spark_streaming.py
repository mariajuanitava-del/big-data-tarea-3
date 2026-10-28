from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg

spark = (
    SparkSession.builder
    .appName("Sensores")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = (
    "sensor_id INT, temperature DOUBLE, "
    "humidity DOUBLE, timestamp LONG"
)

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "earliest")
    .load()
)

data = (
    raw.select(
        from_json(col("value").cast("string"), schema).alias("sensor")
    )
    .select("sensor.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

averages = (
    data.groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio")
    )
)

query = (
    averages.writeStream
    .outputMode("complete")
    .foreachBatch(lambda tabla, lote:
        tabla.coalesce(1).write.mode("overwrite")
        .json("/home/hadoop/resultados_sensores"))
    .option("truncate", "false")
    .option("numRows", "50")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
