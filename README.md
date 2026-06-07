# Databricks Jobs → ClickZetta Lakehouse Studio Tasks

Migrate a **Databricks Jobs multi-task pipeline** (3 tasks, DAG dependencies, cron schedule) to **Lakehouse Studio tasks** via `cz-cli`. **8/8 e2e verified on AWS Singapore.**

## Pipeline

```
ingest_raw → transform_silver → aggregate_gold
(Bronze)       (Silver)          (Gold)
 500 rows       500 rows          115 rows / 5 categories / $12,814.84
```

## Quick Start

```bash
cp .env.example .env

# 1. Upload data + run ZettaPark scripts (local dev)
python3 setup.py

# 2. OR create Studio Tasks (production)
PROFILE=aws_singapore_prod bash 03_lakehouse/setup_tasks.sh

# 3. Verify
python3 e2e.py   # 8/8 ✅
```

## Structure

```
├── 01_source/
│   ├── jobs/ecommerce_etl_job.json   ← original Databricks Job definition
│   └── notebooks/                    ← original PySpark notebooks (4 files)
├── 02_migration/migration-notes.md   ← mapping table + cz-cli commands
├── 03_lakehouse/
│   ├── tasks/                        ← Studio Python tasks (ZettaPark)
│   └── setup_tasks.sh                ← one-click: create + deps + cron + deploy
├── data/                             ← events.csv / products.csv
├── e2e.py                            ← 8 automated checks
└── setup.py                          ← local ZettaPark runner
```

## Key Mappings

| Databricks Jobs | Studio | Notes |
|---|---|---|
| `depends_on: [{task_key}]` | `--dep-tasks '[{"taskId":N,"taskName":"x"}]'` | DAG deps |
| Quartz cron `"0 0 2 * * ?"` | Standard cron `"0 2 * * *"` | Format change |
| `spark` global | `Session via clickzetta_dbutils` | Platform injection |
| `dbutils.notebook.run(nb)` | Task dependencies | Replaced by platform |
| `email_notifications` | Studio monitoring rules | Email/DingTalk/Feishu |

## Studio Task IDs (AWS Singapore)

| Task | ID | Status |
|---|---|---|
| etl_01_ingest_raw | 10143594 | ONLINE, cron=02:00 |
| etl_02_transform_silver | 10144488 | ONLINE, depends on 01 |
| etl_03_aggregate_gold | 10143595 | ONLINE, depends on 02 |
