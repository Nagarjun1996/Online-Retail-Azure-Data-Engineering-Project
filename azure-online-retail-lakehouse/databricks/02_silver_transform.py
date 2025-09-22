from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
exec(open("../databricks/00_config.py").read(), globals())

bronze_delta = spark.read.format("delta").load(f"{SILVER_PATH}bronze_delta/")

df = (
    bronze_delta
      .withColumn("description", F.trim(F.coalesce(F.col("description"), F.lit("Unknown"))))
      .withColumn("is_return", F.col("quantity") < 0)
      .withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)))
      .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(0)))
      .withColumn("revenue", F.col("quantity").cast("double") * F.col("price").cast("double"))
)

(df.write.format("delta").mode("overwrite").partitionBy("year","month")
 .save(f"{SILVER_PATH}orders_silver/"))
print("Silver saved → orders_silver")
