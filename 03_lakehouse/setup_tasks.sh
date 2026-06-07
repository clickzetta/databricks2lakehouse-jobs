#!/bin/bash
# Deploy Studio tasks equivalent to Databricks Jobs pipeline
# Usage: PROFILE=aws_singapore_prod bash setup_tasks.sh

PROFILE=${PROFILE:-aws_singapore_prod}
FOLDER_ID=91047   # ecommerce_etl folder (get via: cz-cli task list-folders)

echo "Creating tasks (type=PYTHON, folder=$FOLDER_ID)..."
ID_01=$(cz-cli task create etl_01_ingest_raw      --type PYTHON --folder $FOLDER_ID --profile $PROFILE --field data.id)
ID_02=$(cz-cli task create etl_02_transform_silver --type PYTHON --folder $FOLDER_ID --profile $PROFILE --field data.id)
ID_03=$(cz-cli task create etl_03_aggregate_gold   --type PYTHON --folder $FOLDER_ID --profile $PROFILE --field data.id)
echo "  Task IDs: $ID_01, $ID_02, $ID_03"

echo "Uploading content..."
cz-cli task save-content $ID_01 --file 03_lakehouse/tasks/01_ingest_raw.py      --profile $PROFILE
cz-cli task save-content $ID_02 --file 03_lakehouse/tasks/02_transform_silver.py --profile $PROFILE
cz-cli task save-content $ID_03 --file 03_lakehouse/tasks/03_aggregate_gold.py   --profile $PROFILE

echo "Setting dependencies (DAG)..."
# Databricks: depends_on: [{task_key: "ingest_raw"}]
# Studio: --dep-tasks '[{"taskId": N, "taskName": "name"}]'
cz-cli task save-config $ID_02 --deps replace --dep-tasks "[{\"taskId\":$ID_01,\"taskName\":\"etl_01_ingest_raw\"}]"  --profile $PROFILE
cz-cli task save-config $ID_03 --deps replace --dep-tasks "[{\"taskId\":$ID_02,\"taskName\":\"etl_02_transform_silver\"}]" --profile $PROFILE

echo "Setting cron schedule (daily at 02:00)..."
# Databricks: schedule.quartz_cron_expression: "0 0 2 * * ?"
# Studio: standard 5-field cron
cz-cli task save-cron $ID_01 --cron "0 2 * * *" --profile $PROFILE

echo "Deploying tasks..."
cz-cli task deploy $ID_01 --profile $PROFILE
cz-cli task deploy $ID_02 --profile $PROFILE
cz-cli task deploy $ID_03 --profile $PROFILE

echo ""
echo "Done! Trigger pipeline:"
echo "  cz-cli task execute $ID_01 --profile $PROFILE"
