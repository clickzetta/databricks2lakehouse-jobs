# Databricks notebook: 03_aggregate_gold
# Daily sales aggregation — Gold layer

from pyspark.sql import functions as F

silver = spark.table("ecommerce.silver.events_enriched")

gold = (silver
    .filter(F.col("event_type") == "purchase")
    .withColumn("sale_date", F.to_date(F.col("event_time")))
    .groupBy("sale_date", "category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("event_id").alias("total_orders"),
        F.countDistinct("user_id").alias("unique_buyers"),
        F.round(F.avg("amount"), 2).alias("avg_order_value")
    )
)

gold.write.mode("overwrite").saveAsTable("ecommerce.gold.daily_sales_by_category")
print(f"Gold: {gold.count()} rows")

# Also run with dbutils.notebook.run pattern (orchestration)
# This is what the orchestration notebook does:
# dbutils.notebook.run("03_aggregate_gold", timeout_seconds=0)
