# Migration Notes: Databricks Jobs → Studio Task DAG

## Concept Mapping

| Databricks Jobs | Lakehouse Studio | Notes |
|---|---|---|
| Job JSON (`tasks: [...]`) | `cz-cli task create + save-config` | Config layer |
| `depends_on: [{task_key}]` | `--dep-tasks '[{"taskId":N,"taskName":"name"}]'` | Same semantics |
| Notebook task (Python) | Studio PYTHON task | Content: ZettaPark |
| SQL task | Studio SQL task | Direct reuse |
| `dbutils.notebook.run(nb)` | Task dependencies (DAG) | Replaced by platform |
| `dbutils.widgets.get(k)` | Studio task parameters | Config via Studio UI |
| `schedule.quartz_cron_expression` | `--cron "0 2 * * *"` | Quartz → standard cron |
| `spark` global | `Session via clickzetta_dbutils` | Platform injection |
| Job cluster | VCluster (auto-managed) | No cluster config needed |
| Email on_failure alert | Studio monitoring rules | Email/DingTalk/Feishu |

## Key cz-cli Commands

```bash
# Create task (--type required: SQL, PYTHON, SHELL...)
cz-cli task create <name> --type PYTHON --folder <folder_id>

# Upload script content
cz-cli task save-content <task_id> --file <path.py>

# Set dependencies (both taskId AND taskName required)
cz-cli task save-config <task_id> --deps replace \
  --dep-tasks '[{"taskId":N,"taskName":"task_name"}]'

# Set cron (standard 5-field, not Quartz)
cz-cli task save-cron <task_id> --cron "0 2 * * *"

# Deploy to production
cz-cli task deploy <task_id>

# Ad-hoc trigger
cz-cli task execute <task_id>
```

## Alert Configuration

Databricks Jobs supports `email_notifications` in Job JSON.
Lakehouse Studio equivalent: monitoring rules (运维监控 → 告警监控).

Supported channels:
- Email (邮件)
- SMS (短信)
- Phone call (电话) — for Critical/Severe levels
- Webhook — DingTalk (钉钉) / Feishu (飞书)

Configure via Studio UI: 告警监控 → 新建监控规则 → 选择任务失败事件 → 配置通知策略

## Verified

- etl_01_ingest_raw (id=10143594): ONLINE, cron=0 2 * * *
- etl_02_transform_silver (id=10144488): ONLINE, depends_on=[01]
- etl_03_aggregate_gold (id=10143595): ONLINE, depends_on=[02]
- Pipeline output: 500→500→115 rows, revenue=12814.84
