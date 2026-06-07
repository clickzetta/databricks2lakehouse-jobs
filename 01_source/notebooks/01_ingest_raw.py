# Databricks notebook: 01_ingest_raw
# Ingests raw events from Volume into Bronze layer

# dbutils.widgets.text("volume_path", "/Volumes/ecommerce/landing/events/")
volume_path = dbutils.widgets.get("volume_path")

df = spark.read.option("header","true").option("inferSchema","true").csv(volume_path)
df.write.mode("overwrite").saveAsTable("ecommerce.bronze.raw_events")

print(f"Ingested {df.count()} rows from {volume_path}")
