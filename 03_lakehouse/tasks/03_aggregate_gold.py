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

silver = session.table("jobs_silver.events_enriched")

# ⬇ Identical to original notebook ⬇
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

gold.write.mode("overwrite").saveAsTable("jobs_gold.daily_sales_by_category")
print(f"Gold: {gold.count()} rows")
