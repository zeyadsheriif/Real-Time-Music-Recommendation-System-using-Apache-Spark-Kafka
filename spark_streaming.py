from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexerModel
import os
import time

spark = SparkSession.builder.appName("StreamingRecs") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

user_indexer = StringIndexerModel.load("user_indexer")
item_indexer = StringIndexerModel.load("item_indexer")
als_model = ALSModel.load("als_model")

schema = StructType([
    StructField("user", StringType(), True),
    StructField("item", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

raw_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "music_events").option("startingOffsets", "latest").load()

parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*").withColumn("event_time", current_timestamp())
watermarked_stream = parsed_stream.withWatermark("event_time", "30 seconds")

def process_batch(batch_df, batch_id):
    if len(batch_df.head(1)) > 0:
        start_time = time.time()
        
        try:
            indexed_df = user_indexer.transform(batch_df)
            indexed_df = item_indexer.transform(indexed_df)
            indexed_df = indexed_df.withColumn("user_id", col("user_id").cast("integer")).withColumn("item_id", col("item_id").cast("integer"))
            
            unique_users = indexed_df.select("user_id", "user").distinct()
            top_5_recs = als_model.recommendForUserSubset(unique_users, 5)
            
            final_recs = top_5_recs.join(unique_users, on="user_id", how="inner") \
                .withColumn("clean_items", expr("transform(recommendations, x -> concat('Item_', cast(x.item_id as string)))")) \
                .withColumn("recommendations", expr("concat_ws(', ', clean_items)")) \
                .select("user", "recommendations")
                
            final_recs.toPandas().to_csv("dashboard_recs.csv", mode='w', header=True, index=False)
        except Exception as e:
            pass

        alerts = batch_df.filter(col("rating") > 4.5)
        if len(alerts.head(1)) > 0:
            alerts.toPandas().to_csv("dashboard_alerts.csv", mode='a', header=not os.path.exists("dashboard_alerts.csv"), index=False)
            
        latency = round((time.time() - start_time) * 1000, 2)
        with open("dashboard_latency.txt", "w") as f:
            f.write(str(latency))

query1 = parsed_stream.writeStream.foreachBatch(process_batch).trigger(processingTime="2 seconds").start()

windowed_metrics = watermarked_stream.groupBy(window(col("event_time"), "30 seconds", "10 seconds"), col("item")).agg({"rating": "avg", "item": "count"}).withColumnRenamed("avg(rating)", "avg_rating").withColumnRenamed("count(item)", "interaction_count").withColumn("engagement_score", col("avg_rating") * col("interaction_count"))

def process_item_window(batch_df, batch_id):
    if len(batch_df.head(1)) > 0:
        batch_df.orderBy(col("engagement_score").desc()).toPandas().to_csv("dashboard_trending.csv", mode='w', header=True, index=False)

query2 = windowed_metrics.writeStream.outputMode("update").foreachBatch(process_item_window).trigger(processingTime="2 seconds").start()

user_activity = watermarked_stream.groupBy(window(col("event_time"), "30 seconds", "10 seconds"), col("user")).count().withColumnRenamed("count", "activity_count")

def process_user_window(batch_df, batch_id):
    if len(batch_df.head(1)) > 0:
        batch_df.orderBy(col("activity_count").desc()).toPandas().to_csv("dashboard_users.csv", mode='w', header=True, index=False)

query3 = user_activity.writeStream.outputMode("update").foreachBatch(process_user_window).trigger(processingTime="2 seconds").start()

spark.streams.awaitAnyTermination()
