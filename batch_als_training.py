from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("BatchALS").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("music_ratings.csv", inferSchema=True).toDF("user", "item", "rating", "timestamp")

user_indexer = StringIndexer(inputCol="user", outputCol="user_id", handleInvalid="keep").fit(df)
item_indexer = StringIndexer(inputCol="item", outputCol="item_id", handleInvalid="keep").fit(df)

df = user_indexer.transform(df)
df = item_indexer.transform(df)
df = df.withColumn("user_id", df["user_id"].cast(IntegerType())).withColumn("item_id", df["item_id"].cast(IntegerType()))

train, test = df.randomSplit([0.8, 0.2], seed=42)

als = ALS(maxIter=10, regParam=0.1, userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(train)

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
predictions = model.transform(test)
rmse = evaluator.evaluate(predictions)

if rmse > 1.5:
    als = ALS(maxIter=20, regParam=0.05, userCol="user_id", itemCol="item_id", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(train)

user_indexer.write().overwrite().save("user_indexer")
item_indexer.write().overwrite().save("item_indexer")
model.write().overwrite().save("als_model")

spark.stop()
