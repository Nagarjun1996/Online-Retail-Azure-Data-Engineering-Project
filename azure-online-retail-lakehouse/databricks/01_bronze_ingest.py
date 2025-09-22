from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import re
spark = SparkSession.getActiveSession()

exec(open("../databricks/00_config.py").read(), globals())

raw_path = f"{BRONZE_PATH}csv/"

df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(raw_path)
)

for c in df.columns:
    df = df.withColumnRenamed(c, re.sub(r'\W+', '_', c.strip()).lower())

df = (
    df.withColumn("quantity", F.col("quantity").cast("int"))
      .withColumn("price", F.coalesce(F.col("unitprice"), F.col("price")).cast("double"))
      .withColumn("invoicedate", F.to_timestamp("invoicedate"))
      .withColumn("year", F.year("invoicedate"))
      .withColumn("month", F.month("invoicedate"))
)

(df.write.format("delta").mode("overwrite").partitionBy("year","month")
 .save(f"{SILVER_PATH}bronze_delta/"))
print("Bronze → silver/bronze_delta complete")
