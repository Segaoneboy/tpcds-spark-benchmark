#!/usr/bin/env bash
# T07: Универсальный скрипт установки и проверки PySpark, JDBC и Avro (local[*])
# Скрипт подходит и для локальной машины и для сервера/виртуалки
# особенность в том, что скрипт не трогает установленные версии джавы, чтобы не нарушать работу других проектов

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-$HOME/tpcds-spark-benchmark}"
SPARK_VERSION="3.5.4"
SCALA_VERSION="2.12"
PG_JDBC_VERSION="42.7.3"


echo "[1/6] Проверка и установка системных пакетов (Java 17, venv, wget)"


# Запоминаем текущую системную Java, чтобы не сломать настройки на локальной машине
ORIG_JAVA_BIN=""
if command -v java >/dev/null 2>&1; then
    ORIG_JAVA_BIN="$(readlink -f "$(command -v java)" || true)"
fi

sudo apt-get update -y
sudo apt-get install -y openjdk-17-jdk-headless python3-venv python3-pip wget

# Если на локальной машине стояла другая Java по умолчанию — возвращаем ее для системы
if [[ -n "$ORIG_JAVA_BIN" && -x "$ORIG_JAVA_BIN" ]]; then
    CURRENT_JAVA_BIN="$(readlink -f "$(command -v java)" || true)"
    if [[ "$ORIG_JAVA_BIN" != "$CURRENT_JAVA_BIN" ]]; then
        echo "Восстанавливаем вашу системную Java по умолчанию вне проекта: $ORIG_JAVA_BIN"
        sudo update-alternatives --set java "$ORIG_JAVA_BIN" || true
    fi
fi

# Определяем путь к Java 17 (поддержка пакета Arenadata и стандартного OpenJDK)
if [[ -d "/usr/lib/jvm/java-arenadata-openjdk-17" ]]; then
    JAVA17_HOME="/usr/lib/jvm/java-arenadata-openjdk-17"
elif [[ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]]; then
    JAVA17_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
elif [[ -d "/usr/lib/jvm/java-17-openjdk-arm64" ]]; then
    JAVA17_HOME="/usr/lib/jvm/java-17-openjdk-arm64"
else
    echo "ОШИБКА: Не удалось найти каталог Java 17 в /usr/lib/jvm/"
    exit 1
fi

echo "Найдена Java 17 для Spark: $JAVA17_HOME"


echo "[2/6] Создание структуры каталогов проекта (по стандарту Т03)"


mkdir -p "$PROJECT_DIR"/{jars,sql/validation,sql/workloads,scripts,results/raw_metrics,results/aggregated,results/figures,plans/physical_plans,plans/spark_ui_screenshots,logs/spark_events,logs/app_runs,docs/report,docs/presentation}
mkdir -p "$PROJECT_DIR"/data/{raw,small/parquet,small/orc,small/avro,small/csv_gz,large/parquet,large/orc,large/avro,large/csv_gz}

cd "$PROJECT_DIR"
echo "Рабочая директория: $PROJECT_DIR"


echo "[3/6] Настройка изолированного Python-окружения (.venv) и PySpark"


if [[ ! -d "$PROJECT_DIR/.venv" ]]; then
    python3 -m venv "$PROJECT_DIR/.venv"
fi

# Прописываем JAVA_HOME строго внутрь .venv/bin/activate (без дублирования)
if ! grep -q " SPARK_JAVA17_CONFIG" "$PROJECT_DIR/.venv/bin/activate"; then
    cat <<EOF >> "$PROJECT_DIR/.venv/bin/activate"

# SPARK_JAVA17_CONFIG: Изолированная Java 17 для Spark 3.5.4
export JAVA_HOME="$JAVA17_HOME"
export PATH="\$JAVA_HOME/bin:\$PATH"
EOF
fi

# Активируем виртуальное окружение
# shellcheck disable=SC1091
source "$PROJECT_DIR/.venv/bin/activate"

export JAVA_HOME="$JAVA17_HOME"
export PATH="$JAVA_HOME/bin:$PATH"

echo "Активная версия Java внутри .venv:"
java -version

pip install --upgrade pip
pip install "pyspark==${SPARK_VERSION}" pandas pyarrow psutil tabulate

cat <<EOF > "$PROJECT_DIR/requirements.txt"
pyspark==${SPARK_VERSION}
pandas
pyarrow
psutil
tabulate
EOF


echo "[4/6] Скачивание JAR-библиотек: PostgreSQL JDBC и модуля Spark Avro"

PG_JAR="$PROJECT_DIR/jars/postgresql-${PG_JDBC_VERSION}.jar"
AVRO_JAR="$PROJECT_DIR/jars/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar"

if [[ ! -f "$PG_JAR" ]]; then
    wget -q --show-progress -O "$PG_JAR" \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"
fi

if [[ ! -f "$AVRO_JAR" ]]; then
    wget -q --show-progress -O "$AVRO_JAR" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar"
fi


echo "[5/6] Установка JAR-библиотек в каталог PySpark (под любой Python)"


# Автоматически вычисляем точный путь к папке jars внутри установленного pyspark
PYSPARK_JARS_DIR="$(python3 -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"
echo "Каталог библиотек PySpark: $PYSPARK_JARS_DIR"

cp -f "$PG_JAR" "$AVRO_JAR" "$PYSPARK_JARS_DIR/"
echo "Оба JAR-файла успешно скопированы."


echo "[6/6] Проверочный запуск Spark в режиме local[*] (Закрытие Т07)"


python3 - <<'PYEOF'
import os
import shutil
from pyspark.sql import SparkSession

event_log_dir = os.path.expanduser("~/tpcds-spark-benchmark/logs/spark_events")
os.makedirs(event_log_dir, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("T07_Verification")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.orc.enableVectorizedReader", "true")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{event_log_dir}")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n------------------------------------------------------------------")
print("1. SPARK VERSION            :", spark.version)
print("2. SPARK MASTER             :", spark.sparkContext.master)
print("3. DEFAULT PARALLELISM      :", spark.sparkContext.defaultParallelism)

# Проверка записи и чтения Avro
tmp_avro = "/tmp/t07_test_avro"
shutil.rmtree(tmp_avro, ignore_errors=True)
df = spark.range(0, 1000).withColumnRenamed("id", "test_id")
df.write.mode("overwrite").format("avro").save(tmp_avro)
avro_cnt = spark.read.format("avro").load(tmp_avro).count()
print("4. AVRO MODULE OK, ROWS     :", avro_cnt)
shutil.rmtree(tmp_avro, ignore_errors=True)

# Проверка загрузки драйвера PostgreSQL JDBC в JVM
driver_cls = spark.sparkContext._jvm.java.lang.Class.forName("org.postgresql.Driver")
print("5. POSTGRES JDBC DRIVER     :", driver_cls.getName())
print("------------------------------------------------------------------\n")

spark.stop()
PYEOF



echo "Для работы в новых окнах терминала активируйте окружение командой:"
echo "  source $PROJECT_DIR/.venv/bin/activate"

