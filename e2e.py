#!/usr/bin/env python3
"""Verify Databricks Jobs → Studio task migration results."""

import subprocess, json, os, sys

PROFILE = os.getenv("CLICKZETTA_PROFILE", "aws_singapore_prod")

def sql(q):
    for _ in range(2):
        try:
            r = subprocess.run(["cz-cli","sql",q,"--profile",PROFILE,"--sync"],
                               capture_output=True,text=True,cwd="/tmp",timeout=60)
            return json.loads(r.stdout) if r.stdout.strip() else {}
        except subprocess.TimeoutExpired:
            pass
    return {}

def n(t): return sql(f"SELECT COUNT(*) FROM {t}").get("rows",[[-1]])[0][0]
def ok(r): return "error" not in r

passed = failed = 0
def check(label, actual, expected, op="=="):
    global passed, failed
    ok_ = (actual == expected) if op == "==" else (actual >= expected)
    if ok_: passed += 1
    else:   failed += 1
    print(f"{'\u2705' if ok_ else f'\u274C EXP={op}{expected}'}  {label}: {actual}")

print("=== Bronze Layer ===")
check("raw_events",  n("jobs_bronze.raw_events"), 500)
check("products",    n("jobs_bronze.products"),    30)

print("\n=== Silver Layer ===")
check("events_enriched", n("jobs_silver.events_enriched"), 500)

print("\n=== Gold Layer ===")
check("daily_sales rows", n("jobs_gold.daily_sales_by_category"), 115)
r = sql("SELECT ROUND(SUM(total_revenue),2) FROM jobs_gold.daily_sales_by_category")
check("total_revenue", r.get("rows",[[-1]])[0][0], 12814.84)
r2 = sql("SELECT SUM(total_orders) FROM jobs_gold.daily_sales_by_category")
check("total_orders", r2.get("rows",[[-1]])[0][0], 119)
r3 = sql("SELECT COUNT(DISTINCT category) FROM jobs_gold.daily_sales_by_category")
check("categories", r3.get("rows",[[-1]])[0][0], 5)

print("\n=== Studio Task DAG ===")
# Verify tasks are ONLINE
r4 = subprocess.run(["cz-cli","task","list","--profile",PROFILE,"--page-size","30"],
                    capture_output=True,text=True,timeout=30)
try:
    tasks_data = json.loads(r4.stdout).get("data",[])
    task_ids = {10143594, 10144488, 10143595}
    online_tasks = [t for t in tasks_data if t["task_id"] in task_ids and t.get("task_edit_state")==20]
    check("all 3 tasks ONLINE", len(online_tasks), 3)
except:
    print("  \u274C could not verify task status")
    failed += 1

print(f"\n{passed}/{passed+failed} passed", "\u2705" if failed==0 else f"\u274C {failed} FAILED")
sys.exit(0 if failed==0 else 1)
