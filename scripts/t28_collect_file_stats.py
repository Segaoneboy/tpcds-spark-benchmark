#!/usr/bin/env python3
"""
Сбор статистики по файлам для всех вариантов хранения store_sales.

Зависит от задач записи форматов (w2_etl_formats.py):
  T19 parquet large, T21 orc large, T23 avro large, T25 csv large, T27 csv_gz large
  (и соответствующие small: T18, T20, T22, T24, T26).

Что делает:
  1. Для каждого формата (parquet, orc, avro, csv_plain, csv_gz) и варианта (small, large)
     считает число файлов, размер каталога и размеры отдельных файлов.
  2. Отдельно проверяет csv_gz/large: один файл, целостность gzip, число строк,
     степень сжатия, согласованность с csv_plain/large и с эталоном из PostgreSQL.

Запуск из корня репозитория:
  python3 scripts/collect_file_stats.py
  python3 scripts/collect_file_stats.py --base data/formats --expected-rows 143997065
  python3 scripts/collect_file_stats.py --skip-content     # без чтения содержимого gzip (быстро)

Результаты (по умолчанию в results/raw_metrics/):
  file_stats_summary.csv     - одна строка на (формат, вариант)
  file_stats_files.csv       - одна строка на каждый файл
  csv_gz_large_check.json    - результат отдельной проверки CSV.GZ large

Код возврата: 0 - всё в порядке, 1 - есть критичные расхождения (см. вывод).
"""
import argparse
import csv
import gzip
import json
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (имя каталога формата, ожидаемое расширение для справки)
FORMATS = ["parquet", "orc", "avro", "csv_plain", "csv_gz"]
VARIANTS = ["small", "large"]

# w2_etl_formats.py: small = repartition(16), large = coalesce(1)
EXPECTED_FILES = {"small": 16, "large": 1}

TABLE = "store_sales"
CHUNK = 8 * 1024 * 1024


def human(n):
    if n is None:
        return "-"
    n = float(n)
    for unit in ("Б", "КиБ", "МиБ", "ГиБ", "ТиБ"):
        if abs(n) < 1024 or unit == "ТиБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.2f} {unit}"
        n /= 1024


def classify(name):
    """Файл данных Spark: part-*; всё остальное (_SUCCESS, .crc, _committed_*) - служебное."""
    if name.startswith("part-") and not name.endswith(".crc"):
        return "data"
    return "service"


def scan_dir(path):
    """Возвращает список файлов с размерами (рекурсивно)."""
    files = []
    for dirpath, _dirs, names in os.walk(path):
        for name in sorted(names):
            full = Path(dirpath) / name
            st = full.lstat()
            files.append(
                {
                    "path": full,
                    "rel": str(full.relative_to(path)),
                    "kind": classify(name),
                    "size_bytes": st.st_size,
                    # реально занятое место на диске (блоки по 512 байт)
                    "disk_bytes": getattr(st, "st_blocks", 0) * 512,
                }
            )
    return files


def variant_dir(base, fmt, variant):
    return base / fmt / variant / TABLE


def summarize(fmt, variant, path):
    row = {
        "format": fmt,
        "variant": variant,
        "path": str(path),
        "status": "OK",
        "data_files": 0,
        "service_files": 0,
        "expected_data_files": EXPECTED_FILES[variant],
        "files_count_ok": "",
        "data_bytes": 0,
        "service_bytes": 0,
        "dir_bytes": 0,
        "disk_bytes": 0,
        "min_file_bytes": "",
        "median_file_bytes": "",
        "mean_file_bytes": "",
        "max_file_bytes": "",
        "ratio_vs_csv_plain": "",
    }
    if not path.is_dir():
        row["status"] = "MISSING"
        return row, []

    files = scan_dir(path)
    data = [f for f in files if f["kind"] == "data"]
    service = [f for f in files if f["kind"] == "service"]

    row["data_files"] = len(data)
    row["service_files"] = len(service)
    row["data_bytes"] = sum(f["size_bytes"] for f in data)
    row["service_bytes"] = sum(f["size_bytes"] for f in service)
    row["dir_bytes"] = row["data_bytes"] + row["service_bytes"]
    row["disk_bytes"] = sum(f["disk_bytes"] for f in files)
    row["files_count_ok"] = "yes" if len(data) == EXPECTED_FILES[variant] else "NO"

    if data:
        sizes = [f["size_bytes"] for f in data]
        row["min_file_bytes"] = min(sizes)
        row["median_file_bytes"] = int(statistics.median(sizes))
        row["mean_file_bytes"] = int(statistics.mean(sizes))
        row["max_file_bytes"] = max(sizes)
    else:
        row["status"] = "EMPTY"

    return row, files


def reference_row_count(root):
    """Число строк store_sales из эталонных метрик T11 (если файл есть)."""
    ref = root / "results" / "reference_metrics" / "reference_metrics.csv"
    if not ref.is_file():
        return None
    with ref.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if (
                r.get("table_name") == TABLE
                and r.get("metric") == "count"
                and r.get("column_name") == "*"
            ):
                try:
                    return int(r["value"])
                except ValueError:
                    return None
    return None


def check_csv_gz_large(base, csv_plain_large_bytes, expected_rows, skip_content):
    """Отдельная проверка csv_gz/large. Возвращает словарь результата."""
    path = variant_dir(base, "csv_gz", "large")
    checks = []
    info = {"path": str(path)}

    def add(name, ok, detail, level="fail"):
        # level: fail - критично, warn - предупреждение, info - справка
        checks.append({"check": name, "ok": bool(ok), "level": level, "detail": detail})

    if not path.is_dir():
        add("каталог существует", False, f"нет каталога {path}")
        return {"info": info, "checks": checks, "passed": False}

    files = scan_dir(path)
    data = [f for f in files if f["kind"] == "data"]
    service = [f for f in files if f["kind"] == "service"]
    info["service_files"] = [f["rel"] for f in service]

    add("служебный файл _SUCCESS есть", any(f["rel"] == "_SUCCESS" for f in service),
        "запись завершена (_SUCCESS)" if any(f["rel"] == "_SUCCESS" for f in service)
        else "нет _SUCCESS: запись могла оборваться", level="warn")

    add("ровно один файл данных (coalesce(1))", len(data) == 1,
        f"найдено файлов данных: {len(data)}")
    if not data:
        return {"info": info, "checks": checks, "passed": False}

    data.sort(key=lambda f: f["size_bytes"], reverse=True)
    target = data[0]
    info["file"] = target["rel"]
    info["compressed_bytes"] = target["size_bytes"]
    info["compressed_human"] = human(target["size_bytes"])

    add("расширение .csv.gz", target["rel"].endswith(".csv.gz"), target["rel"])
    add("файл не пустой", target["size_bytes"] > 0, human(target["size_bytes"]))

    with open(target["path"], "rb") as fh:
        magic = fh.read(2)
    add("сигнатура gzip (1f 8b)", magic == b"\x1f\x8b", magic.hex())
    if magic != b"\x1f\x8b":
        return {"info": info, "checks": checks, "passed": False}

    if skip_content:
        add("целостность gzip и число строк", True, "пропущено (--skip-content)", level="info")
    else:
        t0 = time.time()
        total = 0
        newlines = 0
        first_line = b""
        last_byte = b""
        try:
            with gzip.open(target["path"], "rb") as gz:
                while True:
                    chunk = gz.read(CHUNK)
                    if not chunk:
                        break
                    if total == 0:
                        first_line = chunk.split(b"\n", 1)[0]
                    total += len(chunk)
                    newlines += chunk.count(b"\n")
                    last_byte = chunk[-1:]
            gz_ok, gz_detail = True, f"прочитано без ошибок за {time.time() - t0:.1f} с"
        except (OSError, EOFError) as exc:
            gz_ok, gz_detail = False, f"{type(exc).__name__}: {exc}"
        add("целостность gzip (CRC32, нет обрыва)", gz_ok, gz_detail)

        if gz_ok:
            lines = newlines + (0 if last_byte == b"\n" or total == 0 else 1)
            rows = max(lines - 1, 0)  # минус строка заголовка (header=true)
            info.update(
                uncompressed_bytes=total,
                uncompressed_human=human(total),
                lines=lines,
                data_rows=rows,
                compression_ratio=round(total / target["size_bytes"], 3),
            )
            header = first_line.decode("utf-8", "replace")
            info["header"] = header
            add("первая строка - заголовок CSV", header.startswith("ss_sold_date_sk"), header[:80])
            add("файл заканчивается переводом строки", last_byte == b"\n", repr(last_byte),
                level="warn")
            add("степень сжатия разумная (>1.5x)", total / target["size_bytes"] > 1.5,
                f"{total / target['size_bytes']:.2f}x", level="warn")

            if expected_rows is not None:
                add("число строк = эталон PostgreSQL", rows == expected_rows,
                    f"в файле {rows}, ожидалось {expected_rows}")
            else:
                add("число строк = эталон PostgreSQL", True,
                    f"эталон не задан (--expected-rows / T11), в файле {rows}", level="info")

            if csv_plain_large_bytes:
                same = total == csv_plain_large_bytes
                add("распакованный размер = csv_plain/large", same,
                    f"gz после распаковки: {total}, csv_plain: {csv_plain_large_bytes}",
                    level="warn")

    # Справка про чтение в Spark: gzip не делится на разделы
    add("gzip нельзя разделить (non-splittable)", True,
        "Spark прочитает файл ровно одной задачей (1 partition), параллелизма нет", level="info")

    passed = all(c["ok"] for c in checks if c["level"] == "fail")
    return {"info": info, "checks": checks, "passed": passed}


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--base", default=str(ROOT / "data" / "formats"),
                    help="корень с форматами (по умолчанию data/formats, как в w2_etl_formats.py)")
    ap.add_argument("--out-dir", default=str(ROOT / "results" / "raw_metrics"))
    ap.add_argument("--expected-rows", type=int, default=None,
                    help="эталонное число строк store_sales (по умолчанию берётся из T11)")
    ap.add_argument("--skip-content", action="store_true",
                    help="не читать содержимое csv_gz/large (только размеры и структура)")
    args = ap.parse_args()

    base = Path(args.base)
    out_dir = Path(args.out_dir)
    expected_rows = args.expected_rows if args.expected_rows is not None else reference_row_count(ROOT)

    summary, per_file = [], []
    for fmt in FORMATS:
        for variant in VARIANTS:
            row, files = summarize(fmt, variant, variant_dir(base, fmt, variant))
            summary.append(row)
            for f in files:
                per_file.append(
                    {
                        "format": fmt,
                        "variant": variant,
                        "file": f["rel"],
                        "kind": f["kind"],
                        "size_bytes": f["size_bytes"],
                        "disk_bytes": f["disk_bytes"],
                    }
                )

    # Сжатие относительно несжатого CSV (по тому же варианту)
    plain = {r["variant"]: r["data_bytes"] for r in summary
             if r["format"] == "csv_plain" and r["status"] == "OK"}
    for r in summary:
        if r["status"] == "OK" and plain.get(r["variant"]):
            r["ratio_vs_csv_plain"] = round(r["data_bytes"] / plain[r["variant"]], 4)

    summary_fields = [
        "format", "variant", "status", "data_files", "expected_data_files", "files_count_ok",
        "service_files", "data_bytes", "service_bytes", "dir_bytes", "disk_bytes",
        "min_file_bytes", "median_file_bytes", "mean_file_bytes", "max_file_bytes",
        "ratio_vs_csv_plain", "path",
    ]
    write_csv(out_dir / "file_stats_summary.csv", summary, summary_fields)
    write_csv(out_dir / "file_stats_files.csv", per_file,
              ["format", "variant", "file", "kind", "size_bytes", "disk_bytes"])

    # --- Сводка в консоль ---
    print(f"\nБаза: {base}")
    hdr = f"{'формат':<10} {'вариант':<6} {'статус':<8} {'файлов':>6} {'ожид.':>5} " \
          f"{'каталог':>12} {'на диске':>12} {'мин':>11} {'медиана':>11} {'макс':>11} {'к csv':>7}"
    print(hdr)
    print("-" * len(hdr))
    for r in summary:
        if r["status"] == "MISSING":
            print(f"{r['format']:<10} {r['variant']:<6} {'НЕТ':<8} (каталог не найден: {r['path']})")
            continue
        ratio = f"{r['ratio_vs_csv_plain']:.3f}" if r["ratio_vs_csv_plain"] != "" else "-"
        flag = "" if r["files_count_ok"] == "yes" else "  <-- число файлов != ожидаемому"
        print(f"{r['format']:<10} {r['variant']:<6} {r['status']:<8} {r['data_files']:>6} "
              f"{r['expected_data_files']:>5} {human(r['dir_bytes']):>12} {human(r['disk_bytes']):>12} "
              f"{human(r['min_file_bytes'] or None):>11} {human(r['median_file_bytes'] or None):>11} "
              f"{human(r['max_file_bytes'] or None):>11} {ratio:>7}{flag}")

    # --- Отдельная проверка csv_gz/large ---
    csv_plain_large = plain.get("large")
    result = check_csv_gz_large(base, csv_plain_large, expected_rows, args.skip_content)
    (out_dir / "csv_gz_large_check.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print("\n=== Проверка csv_gz/large ===")
    marks = {True: "OK  ", False: "FAIL"}
    for c in result["checks"]:
        mark = marks[c["ok"]] if c["level"] == "fail" else ("OK  " if c["ok"] else "WARN")
        if c["level"] == "info":
            mark = "INFO"
        print(f"[{mark}] {c['check']}: {c['detail']}")
    info = result["info"]
    if "uncompressed_bytes" in info:
        print(f"\nСжатый: {info['compressed_human']}, распакованный: {info['uncompressed_human']}, "
              f"сжатие {info['compression_ratio']}x, строк данных: {info['data_rows']}")
    print(f"\nИтог проверки csv_gz/large: {'ПРОЙДЕНА' if result['passed'] else 'НЕ ПРОЙДЕНА'}")
    print(f"Файлы результатов: {out_dir}/file_stats_summary.csv, file_stats_files.csv, "
          f"csv_gz_large_check.json")

    missing = [r for r in summary if r["status"] != "OK"]
    bad_count = [r for r in summary if r["status"] == "OK" and r["files_count_ok"] != "yes"]
    if missing:
        print(f"\nВнимание: не найдено/пусто вариантов: {len(missing)}")
    if bad_count:
        print(f"Внимание: число файлов отличается от ожидаемого у {len(bad_count)} вариантов")
    return 0 if result["passed"] and not missing else 1


if __name__ == "__main__":
    sys.exit(main())
