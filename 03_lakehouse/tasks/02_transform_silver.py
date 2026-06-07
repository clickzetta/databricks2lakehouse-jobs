# Studio Python task — equivalent to Databricks notebook in Jobs
from clickzetta_dbutils import get_active_lakehouse_engine
from clickzetta.zettapark.session import Session
from urllib.parse import urlparse, parse_qs

engine = get_active_lakehouse_engine(schema="quick_start")
url_str = str(engine.url)
parsed = urlparse(url_str.replace('clickzetta://', 'https://'))
params = parse_qs(parsed.query)
parts = parsed.hostname.split('.', 1)

session = Session.builder.configs({
    "service":     parts[1],
    "instance":    parts[0],
    "magic_token": params['magic_token'][0],
    "workspace":   parsed.path.lstrip('/'),
    "schema":      params.get('schema', ['quick_start'])[0],
    "vcluster":    params.get('virtualcluster', ['DEFAULT'])[0],
}).getOrCreate()

from clickzetta.zettapark import functions as F

# spark.table("ecommerce.bronze.X") → session.table("jobs_bronze.X")
events   = session.table("jobs_bronze.raw_events")
products = session.table("jobs_bronze.products")

# ⬇ DataFrame logic IDENTICAL to original notebook ⬇
events_clean = events.filter(
    F.col("event_id").isNotNull() &
    F.col("event_time").isNotNull()
)

silver = events_clean.join(
    products.select("product_id", "name", "category", "price"),
    "product_id", "left"
).withColumn(
    "revenue", F.when(F.col("event_type") == "purchase", F.col("amount")).otherwise(F.lit(0.0))
)

silver.write.mode("overwrite").saveAsTable("jobs_silver.events_enriched")
print(f"Silver: {silver.count()} rows")
