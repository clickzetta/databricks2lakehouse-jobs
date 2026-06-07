# Databricks notebook: 02_transform_silver
# Clean events, join with products, create silver layer

from pyspark.sql import functions as F

events = spark.table("ecommerce.bronze.raw_events")
products = spark.table("ecommerce.bronze.products")

# Filter invalid events
events_clean = events.filter(
    (F.col("event_id").isNotNull()) &
    (F.col("event_time").isNotNull())
)

# Enrich with product info
silver = events_clean.join(
    products.select("product_id", "name", "category", "price"),
    "product_id", "left"
).withColumn(
    "revenue", F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(0.0)
)

silver.write.mode("overwrite").saveAsTable("ecommerce.silver.events_enriched")
print(f"Silver: {silver.count()} rows")
