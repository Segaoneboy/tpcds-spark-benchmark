#!/usr/bin/env bash
# ============================================================================
# T11: зафиксировать эталонные метрики (COUNT/SUM/MIN/MAX) и дамп схемы.
# Работает на macOS и Linux. Нужен установленный PostgreSQL (psql, pg_dump).
#
# Запуск из корня репозитория:
#   ./scripts/t11_reference_metrics.sh
# Подключение настраивается переменными окружения (значения по умолчанию ниже):
#   PGHOST=localhost PGPORT=5432 PGUSER=$USER PGDATABASE=tpcds ./scripts/t11_reference_metrics.sh
# ============================================================================
set -euo pipefail

# Корень репозитория = папка выше scripts/
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-$USER}"
export PGDATABASE="${PGDATABASE:-tpcds}"

SQL_FILE="$ROOT_DIR/sql/validation/t11_reference_metrics.sql"
OUT_DIR="$ROOT_DIR/results/reference_metrics"
METRICS_CSV="$OUT_DIR/reference_metrics.csv"
SCHEMA_DUMP="$OUT_DIR/schema_dump.sql"

mkdir -p "$OUT_DIR"

# Проверяем, что нужные программы есть
for tool in psql pg_dump; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Ошибка: не найден '$tool'. Установите PostgreSQL (см. docs/t09_t11_instruction_macos.md, шаг 1)." >&2
    exit 1
  }
done

# Проверяем соединение с базой
psql -Atqc "SELECT 1" >/dev/null || {
  echo "Ошибка: не удалось подключиться к базе '$PGDATABASE' на $PGHOST:$PGPORT пользователем '$PGUSER'." >&2
  echo "Проверьте, что Postgres запущен (brew services list) и база создана (createdb $PGDATABASE)." >&2
  exit 1
}

# Проверяем, что таблицы не пустые: иначе метрики будут бессмысленными (T11 зависит от T10)
rows=$(psql -Atqc "SELECT COUNT(*) FROM store_sales")
if [ "$rows" -eq 0 ]; then
  echo "Предупреждение: store_sales пуста. Сначала нужно заполнить базу (задача T10)." >&2
  echo "Метрики всё равно будут записаны, но они нулевые." >&2
fi

# 1) Метрики -> CSV с заголовком
SQL_BODY="$(cat "$SQL_FILE")"
psql -v ON_ERROR_STOP=1 -qc "\\copy ($(echo "$SQL_BODY" | sed '/^--/d;s/;[[:space:]]*$//')) TO '$METRICS_CSV' WITH (FORMAT csv, HEADER true)"
echo "Метрики записаны: $METRICS_CSV ($(($(wc -l < "$METRICS_CSV") - 1)) строк)"

# 2) Дамп схемы (только структура: таблицы, PK, FK, индексы; без данных)
pg_dump --schema-only --no-owner --no-privileges \
  -t date_dim -t item -t store_sales \
  -f "$SCHEMA_DUMP"
echo "Дамп схемы записан: $SCHEMA_DUMP"

# 3) Версия Postgres — пригодится для отчёта
psql -Atqc "SELECT version()" > "$OUT_DIR/postgres_version.txt"
echo "Версия PostgreSQL: $(cat "$OUT_DIR/postgres_version.txt")"
