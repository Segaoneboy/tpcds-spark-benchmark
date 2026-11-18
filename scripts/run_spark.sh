#!/usr/bin/env bash
set -euo pipefail
SCRIPT_PATH="${1:?Укажите путь к python-скрипту, например scripts/t30_capture_plans.py}"

[ -f configs/env.local.sh ] && source configs/env.local.sh || source configs/env.example.sh

spark3-submit \
  --master "${SPARK_MASTER:-yarn}" \
  --deploy-mode "${DEPLOY_MODE:-client}" \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --executor-memory "${EXECUTOR_MEMORY:-1536m}" \
  --executor-cores "${EXECUTOR_CORES:-2}" \
  --jars jars/postgresql-42.7.3.jar,jars/spark-avro_2.13-3.5.4.jar \
  "${SCRIPT_PATH}"
