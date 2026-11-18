#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCALE="${SCALE:-5}"
RAW_DIR="$ROOT_DIR/data/raw"
KIT_DIR="/tmp/tpcds-kit"
DB_NAME="tpcds"

echo "=== [1/5] Проверка PostgreSQL ==="
sudo systemctl enable --now postgresql
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'postgres';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'" | grep -q 1 \
  || sudo -u postgres createdb "${DB_NAME}"

echo "=== [2/5] Сборка dsdgen ==="
if [[ ! -x "$KIT_DIR/tools/dsdgen" ]]; then
    rm -rf "$KIT_DIR"
    git clone --depth 1 https://github.com/gregrahn/tpcds-kit.git "$KIT_DIR"
    # В Makefile tpcds-kit переопределяет CC, поэтому прописываем -fcommon прямо в Makefile
    sed -i 's/^LINUX_CFLAGS.*/LINUX_CFLAGS = -g -Wall -fcommon/' "$KIT_DIR/tools/Makefile.suite"
    make -C "$KIT_DIR/tools" OS=LINUX
fi

echo "=== [3/5] Генерация данных TPC-DS (SCALE=${SCALE}) ==="
mkdir -p "$RAW_DIR"
rm -f "$RAW_DIR"/*.dat
cd "$KIT_DIR/tools"
./dsdgen -scale "$SCALE" -dir "$RAW_DIR" -table date_dim -rngseed 42
./dsdgen -scale "$SCALE" -dir "$RAW_DIR" -table item -rngseed 42
./dsdgen -scale "$SCALE" -dir "$RAW_DIR" -table store_sales -rngseed 42

for tbl in date_dim item store_sales; do
    sed -i 's/|$//' "$RAW_DIR/${tbl}.dat"
done

echo "=== [4/5] Создание схемы и загрузка в PostgreSQL ==="
cd "$ROOT_DIR"
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$ROOT_DIR/sql/t09_schema.sql"

for tbl in date_dim item store_sales; do
    echo "Загрузка ${tbl}..."
    PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d "$DB_NAME" -v ON_ERROR_STOP=1 \
      -c "\COPY ${tbl} FROM '$RAW_DIR/${tbl}.dat' WITH (FORMAT csv, DELIMITER '|', NULL '', ENCODING 'LATIN1');"
done

echo "=== [5/5] ANALYZE и фиксация метрик T11 ==="
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d "$DB_NAME" -c "ANALYZE date_dim; ANALYZE item; ANALYZE store_sales;"

PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE="$DB_NAME" \
  bash "$ROOT_DIR/scripts/t11_reference_metrics.sh"

rm -f "$RAW_DIR"/*.dat
echo "=== ГОТОВО! ==="
