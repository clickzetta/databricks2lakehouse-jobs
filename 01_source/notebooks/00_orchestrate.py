# Databricks notebook: 00_orchestrate
# Chains notebooks using dbutils.notebook.run()
# This is replaced by Jobs DAG in production, but shown for reference

print("Starting ETL pipeline...")

# Step 1
print("Running ingest...")
dbutils.notebook.run("01_ingest_raw", timeout_seconds=300,
    arguments={"volume_path": "/Volumes/ecommerce/landing/events/"})

# Step 2
print("Running transform...")
dbutils.notebook.run("02_transform_silver", timeout_seconds=600)

# Step 3
print("Running aggregation...")
dbutils.notebook.run("03_aggregate_gold", timeout_seconds=300)

print("Pipeline complete!")
