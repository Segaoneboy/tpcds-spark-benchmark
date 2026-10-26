# TPC-DS Spark Storage Benchmark (`tpcds-spark-benchmark`)

Исследовательский проект по сравнительному анализу эффективности форматов хранения данных (**Parquet**, **ORC**, **Avro**, **CSV.GZ**) и степени сжатия под аналитической нагрузкой в Apache Spark.

## Структура гит-репозитория проекта
```
tpcds-spark-benchmark/    
├── README.md                           
├── requirements.txt                    
│  
├── sql/                                 
│   ├── validation/                    # Проверочные запросы (COUNT, SUM, null-checks)  
│   └── workloads/                     # Зачетные запросы по 4 видам нагрузки  
│  
├── scripts/                             
│  
├── data/                              # Локальные/HDFS директории данных  
│   ├── raw/                           # Исходные .dat файлы от генератора TPC-DS  
│   ├── small/                         # Мелкие файлы  
│   │   ├── parquet/  
│   │   ├── orc/  
│   │   ├── avro/  
│   │   └── csv_gz/  
│   └── large/                         # Оптимальные крупные блоки  
│       ├── parquet/  
│       ├── orc/  
│       ├── avro/  
│       └── csv_gz/  
│  
├── results/                           # Результаты замеров и сырые логи  
│   ├── raw_metrics/                   # Выгрузки метрик в формате CSV/JSON после каждого прогона  
│   ├── aggregated/                    # Сводная таблица (summary) с расчетом медианы и IQR  
│   └── figures/                       # Сгенерированные графики (время, I/O, размер)  
│  
├── plans/                             # Планы выполнения запросов Spark (Explain DAG)  
│   ├── physical_plans/                # Текстовые дампы df.explain(mode="formatted")  
│   └── spark_ui_screenshots/          # Скриншоты DAG и стадий из Spark UI (порт 4040/18080)  
│  
├── logs/                              # Event logs и системные логи  
│   ├── spark_events/                  # Каталог для spark.eventLog.dir (история для History Server)  
│   └── app_runs/                      # Консольные логи запуска скриптов (.log)  
│  
├── docs/                              
│   ├── report/                         
│   └── presentation/                  
```

## Соглашение по неймингу файлов
- все файлы именуются строго в нижнем регистре в нотации snake_case.

1) SQL-запросы (sql/workloads): <workload_id>_<short_desc>.sql

2) Файлы результатов (results/raw_metrics/): run_<scenario>_<format>_<workload_id>_<timestamp>.csv
Поля в CSV: ```run_id, scenario, format, query_id, iteration, run_type(cold/warm), wall_time_ms, cpu_time_ms, input_bytes_read, shuffle_bytes, task_count```

3) Текстовые планы запросов (plans/physical_plans/): plan_<scenario>_<format>_<workload_id>.txt  

4) Event Logs Spark (logs/spark_events/): В конфигурации Spark настраивается стандартный логгер:
```
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "/path/to/tpcds-spark-benchmark/logs/spark_events")
```
Spark автоматически сохраняет файлы в формате app-<timestamp>-<app_id>.

5) Итоговый отчет по практика: лучшим способом будет сохранить итоговый отчет по проделанной работе и презентацию на гитхаб docs/

## Версионирование 
Стандартная схема: ``` VMajor.Minor.Patch```  

## Git-репозиторий
1) Ветки  
main - стабильные скрипты, метрики и финальные версии отчета
feat/<task_name> - рабочая ветка со всеми задачами.
2) .gitignore 
Закидывать в гит игнор все тяжелые файлы: данные тестов
В .gitignore попадает директория data
```
*.dat, *.parquet,*.orc,*.avro,*.gz
.venv/ 
```

