#!/usr/bin/env python3
"""Local runner: schemas + volume + data + ZettaPark pipeline."""
import os, sys
from pathlib import Path
from clickzetta.zettapark import Session
from clickzetta.zettapark import functions as F

PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)

session = Session.builder.configs({
    "instance": os.getenv("CZ_INSTANCE","de1cbb4a"),
    "workspace": os.getenv("CZ_WORKSPACE","quick_start"),
    "vcluster":  os.getenv("CZ_VCLUSTER","default"),
    "username":  os.getenv("CZ_USERNAME",""),
    "password":  os.getenv("CZ_PASSWORD",""),
    "service":   os.getenv("CZ_SERVICE","https://ap-southeast-1-aws.api.singdata.com"),
}).create()

# Setup
for s in ["jobs_bronze","jobs_silver","jobs_gold","jobs_landing"]:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {s}").collect()
session.sql("CREATE VOLUME IF NOT EXISTS jobs_landing.raw_data").collect()

# Upload data
for csv in ["events.csv","products.csv"]:
    session.file.put(str(PROJECT_ROOT/"data"/csv), "vol://jobs_landing.raw_data/")
    print(f"  Uploaded {csv}")

# Task 01: ingest
df = session.read.option("header","true").csv("vol://jobs_landing.raw_data/events.csv")
df.write.mode("overwrite").saveAsTable("jobs_bronze.raw_events")
df2 = session.read.option("header","true").csv("vol://jobs_landing.raw_data/products.csv")
df2.write.mode("overwrite").saveAsTable("jobs_bronze.products")
print(f"Bronze: raw_events={df.count()}, products={df2.count()}")

# Task 02: transform
events = session.table("jobs_bronze.raw_events")
products = session.table("jobs_bronze.products")
silver = (events.filter(F.col("event_id").isNotNull())
    .join(products.select("product_id","name","category","price"), "product_id", "left")
    .withColumn("revenue", F.when(F.col("event_type")=="purchase",F.col("amount")).otherwise(F.lit(0.0))))
silver.write.mode("overwrite").saveAsTable("jobs_silver.events_enriched")
print(f"Silver: events_enriched={silver.count()}")

# Task 03: aggregate
s = session.table("jobs_silver.events_enriched")
gold = (s.filter(F.col("event_type")=="purchase")
    .withColumn("sale_date", F.to_date(F.col("event_time")))
    .groupBy("sale_date","category")
    .agg(F.round(F.sum("revenue"),2).alias("total_revenue"),
         F.count("event_id").alias("total_orders"),
         F.countDistinct("user_id").alias("unique_buyers"),
         F.round(F.avg("amount"),2).alias("avg_order_value")))
gold.write.mode("overwrite").saveAsTable("jobs_gold.daily_sales_by_category")
print(f"Gold: daily_sales={gold.count()} rows, revenue={session.sql('SELECT ROUND(SUM(total_revenue),2) FROM jobs_gold.daily_sales_by_category').collect()[0][0]}")
print("\nSetup complete! Run python3 e2e.py to verify.")
