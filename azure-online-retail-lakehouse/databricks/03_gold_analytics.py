from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
exec(open("../databricks/00_config.py").read(), globals())

orders = spark.read.format("delta").load(f"{SILVER_PATH}orders_silver/")

by_country = (
    orders.groupBy("country")
          .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"),
               F.count("*").alias("num_rows"))
)
by_month = (
    orders.groupBy("year","month")
          .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
)

by_country.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}mart_revenue_by_country/")
by_month.write.format("delta").mode("overwrite").partitionBy("year","month").save(f"{GOLD_PATH}mart_revenue_by_month/")
print("Gold marts written")
