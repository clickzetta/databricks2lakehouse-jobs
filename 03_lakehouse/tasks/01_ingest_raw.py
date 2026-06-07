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

# dbutils.widgets.get("volume_path") → Studio task parameter
# Configured via cz-cli task save-config --params volume_path=...
volume_path = "vol://jobs_landing.raw_data/"  # Studio Volume path

# spark.read.csv(volume_path) → session.read.csv("vol://...")
df = session.read.option("header","true").csv(f"{volume_path}events.csv")

# saveAsTable("ecommerce.bronze.raw_events") → saveAsTable("jobs_bronze.raw_events")
df.write.mode("overwrite").saveAsTable("jobs_bronze.raw_events")
print(f"Ingested {df.count()} rows")
