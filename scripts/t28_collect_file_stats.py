#!/usr/bin/env python3
"""
T28: статистика файлов по всем вариантам хранения store_sales + отдельная проверка csv_gz/large.

Зависит от записи форматов (w2_etl_formats.py): T19, T21, T23, T25, T27 (large) и T18, T20, T22, T24, T26 (small).

Где лежат данные (как пишет w2_etl_formats.py):
  <BASE>/<layout>/<format>/store_sales      layout: small | large
                                            format: parquet | orc | avro | csv_plain | csv_gz
  BASE по умолчанию берётся из переменной HDFS_BASE_DIR (та же, что в w2_etl_formats.py и configs/env.example.sh),
  если её нет - hdfs:///user/admin/tpcds_benchmark/data.

Что делает:
  1. Для каждого из 10 вариантов: число файлов данных (part-*), служебных файлов (_SUCCESS и т.п.),
     размер каталога, размеры отдельных файлов (мин / медиана / среднее / макс).
  2. Отдельно проверяет csv_gz/large ПРЯМО В HDFS, без копирования на локальный диск:
       hdfs dfs -cat <файл> | gzip -dc      (поток читается кусками по 8 МиБ, память почти не нужна)
     проверяет: один файл, сигнатура gzip, целостность (CRC и конец потока), заголовок, число полей,
     число строк данных, сжатие, совпадение распакованного размера с csv_plain/large.

Параметры CSV должны совпадать с записью в w2_etl_formats.py:
  ("csv", ..., {"header": "true"}) и разделитель НЕ задан -> по умолчанию Spark пишет запятую.
  Поэтому по умолчанию --csv-header true и --csv-sep ','.
  Число строк = (строк в файле) - (1, только если header=true). Имя первой колонки не зашито:
  заголовок распознаётся по виду (имена колонок), а не по конкретному слову.

Скрипту НЕ нужен Spark, запускайте обычным python3 на узле, где есть команда hdfs:
  python3 scripts/collect_file_stats.py
  python3 scripts/collect_file_stats.py --base hdfs:///user/admin/tpcds_benchmark/data --expected-rows 14401601
  python3 scripts/collect_file_stats.py --skip-content          # без чтения содержимого gzip (быстро)
  python3 scripts/collect_file_stats.py --base data/formats     # локальный каталог (для отладки)

Результаты (по умолчанию results/raw_metrics/):
  file_stats_summary.csv     - одна строка на (layout, format)
  file_stats_files.csv       - одна строка на каждый файл
  csv_gz_large_check.json    - результат проверки csv_gz/large

Про числа: в write_metrics.csv поле file_count берётся из `hdfs dfs -count` и включает _SUCCESS
(16 файлов данных + _SUCCESS = 17). Здесь файлы данных и служебные считаются раздельно.

Код возврата: 0 - всё в порядке, 1 - проверка csv_gz/large не пройдена или нет каталогов вариантов,
2 - не удалось обратиться к файловой системе (нет команды hdfs и т.п.).
"""
import argparse
import csv
import gzip
import json
import os
import re
import shlex
import statistics
import subprocess
import sys
import tempfile
import time
import zlib
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent

# Та же переменная и то же значение по умолчанию, что в w2_etl_formats.py
DEFAULT_BASE = os.getenv("HDFS_BASE_DIR", "hdfs:///user/admin/tpcds_benchmark/data")

FORMATS = ["parquet", "orc", "avro", "csv_plain", "csv_gz"]
LAYOUTS = ["small", "large"]
TABLE = "store_sales"

# w2_etl_formats.py: small = repartition(16), large = coalesce(1)
EXPECTED_FILES = {"small": 16, "large": 1}

# Параметры записи CSV в w2_etl_formats.py: {"header": "true"}, разделитель не задан (по умолчанию ',')
CSV_HEADER_DEFAULT = "true"
CSV_SEP_DEFAULT = ","
STORE_SALES_COLUMNS = 23  # число колонок store_sales (sql/schema/t09_schema.sql)

CHUNK = 8 * 1024 * 1024
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def human(n):
    if n is None or n == "":
        return "-"
    n = float(n)
    for unit in ("Б", "КиБ", "МиБ", "ГиБ", "ТиБ"):
        if abs(n) < 1024 or unit == "ТиБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.2f} {unit}"
        n /= 1024


# ----------------------------------------------------------------------------
# Файловые системы: HDFS (через команду hdfs) и локальная (для отладки)
# ----------------------------------------------------------------------------
class FsError(RuntimeError):
    pass


def path_part(p):
    """'/user/x' из 'hdfs://nn:8020/user/x' и 'hdfs:///user/x'; локальные пути остаются как есть."""
    u = urlparse(str(p))
    return u.path if u.scheme else str(p)


def classify(rel):
    """Файл данных Spark: part-* прямо в каталоге. Остальное (_SUCCESS, .crc, вложенное) - служебное."""
    name = rel.rsplit("/", 1)[-1]
    if "/" not in rel and name.startswith("part-") and not name.endswith(".crc"):
        return "data"
    return "service"


class LocalFS:
    name = "local"

    def isdir(self, path):
        return os.path.isdir(path_part(path))

    def list_files(self, path):
        root = path_part(path)
        out = []
        for dirpath, _dirs, names in os.walk(root):
            for n in sorted(names):
                full = os.path.join(dirpath, n)
                rel = os.path.relpath(full, root).replace(os.sep, "/")
                out.append({"rel": rel, "kind": classify(rel), "size_bytes": os.lstat(full).st_size})
        return out

    def usage(self, path, files):
        total = sum(f["size_bytes"] for f in files)
        disk = 0
        for f in files:
            st = os.lstat(os.path.join(path_part(path), f["rel"]))
            disk += getattr(st, "st_blocks", 0) * 512
        return total, disk or total

    def open_binary(self, path):
        return _LocalStream(path_part(path))


class HdfsFS:
    name = "hdfs"

    def __init__(self, hdfs_cmd="hdfs"):
        self.cmd = shlex.split(hdfs_cmd)

    def _run(self, *args):
        try:
            return subprocess.run([*self.cmd, "dfs", *args], capture_output=True, text=True)
        except FileNotFoundError:
            raise FsError(f"команда '{self.cmd[0]}' не найдена: запускайте на узле кластера "
                          f"или укажите --hdfs-cmd")

    def isdir(self, path):
        return self._run("-test", "-d", path).returncode == 0

    def list_files(self, path):
        r = self._run("-ls", "-R", path)
        if r.returncode != 0:
            raise FsError(f"hdfs dfs -ls -R {path}: {r.stderr.strip()[:300]}")
        root = path_part(path).rstrip("/")
        out = []
        for line in r.stdout.splitlines():
            if not line or line.startswith("Found "):
                continue
            # права, репликация, владелец, группа, размер, дата, время, путь
            parts = line.split(None, 7)
            if len(parts) < 8 or parts[0].startswith("d"):
                continue
            p = path_part(parts[7])
            rel = p[len(root) + 1:] if p.startswith(root + "/") else p.rsplit("/", 1)[-1]
            out.append({"rel": rel, "kind": classify(rel), "size_bytes": int(parts[4])})
        return out

    def usage(self, path, files):
        """(размер, место на дисках с учётом репликации) по hdfs dfs -du -s."""
        total = sum(f["size_bytes"] for f in files)
        r = self._run("-du", "-s", path)
        if r.returncode == 0:
            m = re.match(r"\s*(\d+)\s+(\d+)\s", r.stdout)
            if m:
                return int(m.group(1)), int(m.group(2))
            m = re.match(r"\s*(\d+)\s", r.stdout)
            if m:
                return int(m.group(1)), int(m.group(1))
        return total, total

    def open_binary(self, path):
        return _HdfsStream(self.cmd, path)


class _LocalStream:
    def __init__(self, path):
        self.path = path
        self.f = None

    def __enter__(self):
        self.f = open(self.path, "rb")
        return self.f

    def __exit__(self, *exc):
        self.f.close()

    def check(self):
        pass


class _HdfsStream:
    """hdfs dfs -cat <файл>: поток байт без сохранения на локальный диск."""

    def __init__(self, cmd, path):
        self.cmd, self.path = cmd, path
        self.proc = self.err = None
        self.rc = None

    def __enter__(self):
        self.err = tempfile.TemporaryFile()
        try:
            self.proc = subprocess.Popen([*self.cmd, "dfs", "-cat", self.path],
                                         stdout=subprocess.PIPE, stderr=self.err)
        except FileNotFoundError:
            raise FsError(f"команда '{self.cmd[0]}' не найдена")
        return self.proc.stdout

    def __exit__(self, *exc):
        self.proc.stdout.close()
        try:
            self.rc = self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.rc = self.proc.wait()

    def check(self):
        """Вызывать после того, как поток прочитан до конца: убеждаемся, что hdfs не оборвался."""
        if self.rc not in (0, None):
            self.err.seek(0)
            tail = self.err.read().decode("utf-8", "replace").strip()[-300:]
            raise FsError(f"hdfs dfs -cat завершился с кодом {self.rc}: {tail}")


def make_fs(base, hdfs_cmd):
    scheme = urlparse(base).scheme
    return HdfsFS(hdfs_cmd) if scheme in ("hdfs", "viewfs", "webhdfs") else LocalFS()


def variant_path(base, layout, fmt):
    return f"{base.rstrip('/')}/{layout}/{fmt}/{TABLE}"


# ----------------------------------------------------------------------------
# Статистика по вариантам
# ----------------------------------------------------------------------------
def summarize(fs, fmt, layout, path):
    row = {
        "layout": layout, "format": fmt, "path": path, "status": "OK",
        "data_files": 0, "expected_data_files": EXPECTED_FILES[layout], "files_count_ok": "",
        "service_files": 0, "data_bytes": 0, "service_bytes": 0, "dir_bytes": 0, "disk_bytes": 0,
        "min_file_bytes": "", "median_file_bytes": "", "mean_file_bytes": "", "max_file_bytes": "",
        "ratio_vs_csv_plain": "",
    }
    if not fs.isdir(path):
        row["status"] = "MISSING"
        return row, []

    files = fs.list_files(path)
    data = [f for f in files if f["kind"] == "data"]
    service = [f for f in files if f["kind"] == "service"]
    row["data_files"], row["service_files"] = len(data), len(service)
    row["data_bytes"] = sum(f["size_bytes"] for f in data)
    row["service_bytes"] = sum(f["size_bytes"] for f in service)
    row["dir_bytes"] = row["data_bytes"] + row["service_bytes"]
    row["disk_bytes"] = fs.usage(path, files)[1]
    row["files_count_ok"] = "yes" if len(data) == EXPECTED_FILES[layout] else "NO"
    if data:
        sizes = [f["size_bytes"] for f in data]
        row.update(min_file_bytes=min(sizes), median_file_bytes=int(statistics.median(sizes)),
                   mean_file_bytes=int(statistics.mean(sizes)), max_file_bytes=max(sizes))
    else:
        row["status"] = "EMPTY"
    return row, files


def reference_row_count(root):
    """Число строк store_sales из эталона T11 (results/reference_metrics/reference_metrics.csv)."""
    ref = root / "results" / "reference_metrics" / "reference_metrics.csv"
    if not ref.is_file():
        return None
    with ref.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r.get("table_name") == TABLE and r.get("metric") == "count" and r.get("column_name") == "*":
                try:
                    return int(r["value"])
                except ValueError:
                    return None
    return None


# ----------------------------------------------------------------------------
# Проверка csv_gz/large
# ----------------------------------------------------------------------------
def check_first_lines(first, second, header, sep, expected_cols):
    """
    Проверка заголовка и числа полей по первым двум строкам (str). Возвращает список проверок.
    Не завязана на имя конкретной колонки: заголовок = все поля похожи на имена колонок.
    """
    out = []

    def add(name, ok, detail, level="fail"):
        out.append({"check": name, "ok": bool(ok), "level": level, "detail": detail})

    f0 = first.split(sep)
    f1 = second.split(sep) if second is not None else None
    looks_like_header = all(IDENT.match(x) for x in f0)
    sample = first[:80]

    # Строка начинается с буквы, но не делится на имена по заданному разделителю: заголовок, но другой sep
    other_sep_header = (not looks_like_header) and bool(re.match(r"^[A-Za-z_]", first))

    if header:
        if looks_like_header:
            detail = sample
        elif other_sep_header:
            detail = (f"первая строка похожа на заголовок, но не делится по {sep!r}: {sample!r}. "
                      f"Задайте верный разделитель через --csv-sep")
        else:
            detail = (f"заголовок ожидался, но первая строка похожа на данные: {sample!r}. "
                      f"Если файл писался без header, запустите с --csv-header false")
        add("первая строка - заголовок (header=true)", looks_like_header, detail)
    else:
        add("первая строка - данные (header=false)", not looks_like_header,
            sample if not looks_like_header else
            f"первая строка похожа на заголовок: {sample!r}. Если файл писался с header=true, "
            f"запустите с --csv-header true, иначе одна строка будет посчитана лишней", level="warn")

    if f1 is not None:
        add(f"число полей: первая и вторая строки совпадают (разделитель {sep!r})", len(f0) == len(f1),
            f"{len(f0)} и {len(f1)}. Если разделитель другой, задайте --csv-sep")
    if expected_cols:
        add(f"число колонок = {expected_cols}", len(f0) == expected_cols, f"в первой строке {len(f0)}")
    return out


def count_rows(lines, header):
    """Строки данных = строки файла минус заголовок (только если header=true)."""
    return max(lines - (1 if header else 0), 0)


def check_csv_gz_large(fs, path, csv_plain_large_bytes, expected_rows, header, sep, expected_cols,
                       skip_content):
    checks, info = [], {"path": path, "csv_header": header, "csv_sep": sep}

    def add(name, ok, detail, level="fail"):
        checks.append({"check": name, "ok": bool(ok), "level": level, "detail": detail})

    if not fs.isdir(path):
        add("каталог существует", False, f"нет каталога {path}")
        return {"info": info, "checks": checks, "passed": False}

    files = fs.list_files(path)
    data = [f for f in files if f["kind"] == "data"]
    service = [f for f in files if f["kind"] == "service"]
    info["service_files"] = [f["rel"] for f in service]

    has_success = any(f["rel"] == "_SUCCESS" for f in service)
    add("служебный файл _SUCCESS есть", has_success,
        "запись завершена" if has_success else "нет _SUCCESS: запись могла оборваться", level="warn")
    leftovers = [f["rel"] for f in service if "/" in f["rel"]]
    add("нет вложенных/временных файлов", not leftovers,
        "ок" if not leftovers else f"найдено {len(leftovers)}, например {leftovers[0]}", level="warn")
    add("ровно один файл данных (coalesce(1))", len(data) == 1, f"найдено файлов данных: {len(data)}")
    if len(data) != 1:
        return {"info": info, "checks": checks, "passed": False}

    target = data[0]
    full = f"{path.rstrip('/')}/{target['rel']}"
    info.update(file=target["rel"], compressed_bytes=target["size_bytes"],
                compressed_human=human(target["size_bytes"]))
    add("расширение .csv.gz", target["rel"].endswith(".csv.gz"), target["rel"])
    add("файл не пустой", target["size_bytes"] > 0, human(target["size_bytes"]))
    if target["size_bytes"] == 0:
        return {"info": info, "checks": checks, "passed": False}

    if skip_content:
        try:
            with fs.open_binary(full) as raw:
                magic = raw.read(2)
        except FsError as exc:
            add("чтение файла из HDFS", False, str(exc))
            return {"info": info, "checks": checks, "passed": False}
        add("сигнатура gzip (1f 8b)", magic == b"\x1f\x8b", magic.hex())
        add("целостность gzip, заголовок, число строк", True, "пропущено (--skip-content)", level="info")
        return {"info": info, "checks": checks,
                "passed": all(c["ok"] for c in checks if c["level"] == "fail")}

    t0 = time.time()
    total = newlines = 0
    last = b""
    head_buf = b""
    err = None
    try:
        stream = fs.open_binary(full)
        with stream as raw:
            try:
                with gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                    while True:
                        chunk = gz.read(CHUNK)
                        if not chunk:
                            break
                        if len(head_buf) < 65536:
                            head_buf += chunk[:65536]
                        total += len(chunk)
                        newlines += chunk.count(b"\n")
                        last = chunk[-1:]
            except (OSError, EOFError, zlib.error) as exc:
                err = f"{type(exc).__name__}: {exc}"
        if err is None:
            stream.check()
    except FsError as exc:
        add("чтение файла из HDFS", False, str(exc))
        return {"info": info, "checks": checks, "passed": False}

    not_gzip = err is not None and "Not a gzipped file" in err
    add("сигнатура gzip (1f 8b)", not not_gzip, err if not_gzip else "ок")
    if not_gzip:
        return {"info": info, "checks": checks, "passed": False}
    add("целостность gzip (CRC32, нет обрыва)", err is None,
        err or f"прочитано без ошибок за {time.time() - t0:.1f} с")
    if err:
        return {"info": info, "checks": checks, "passed": False}

    lines = newlines + (0 if last == b"\n" or total == 0 else 1)
    rows = count_rows(lines, header == "true")
    info.update(uncompressed_bytes=total, uncompressed_human=human(total), lines=lines,
                header_lines=1 if header == "true" else 0, data_rows=rows,
                compression_ratio=round(total / target["size_bytes"], 3))

    parts = head_buf.decode("utf-8", "replace").split("\n")
    first = parts[0]
    second = parts[1] if len(parts) > 1 and parts[1] != "" else None
    info["first_line"] = first[:200]
    checks.extend(check_first_lines(first, second, header == "true", sep, expected_cols))

    add("файл заканчивается переводом строки", last == b"\n", repr(last), level="warn")
    add("степень сжатия разумная (>1.5x)", total / target["size_bytes"] > 1.5,
        f"{total / target['size_bytes']:.2f}x", level="warn")
    how = f"{lines} строк в файле, из них заголовок {1 if header == 'true' else 0}"
    if expected_rows is not None:
        add("число строк данных = эталон PostgreSQL (T11)", rows == expected_rows,
            f"{rows} ({how}), ожидалось {expected_rows}")
    else:
        add("число строк данных = эталон PostgreSQL (T11)", True,
            f"эталон не задан (--expected-rows / T11), в файле {rows} ({how})", level="info")
    if csv_plain_large_bytes:
        add("распакованный размер = csv_plain/large", total == csv_plain_large_bytes,
            f"после распаковки {total}, csv_plain/large {csv_plain_large_bytes}", level="warn")
    add("gzip нельзя разделить (non-splittable)", True,
        "Spark читает файл одной задачей (1 partition), параллелизма нет", level="info")

    passed = all(c["ok"] for c in checks if c["level"] == "fail")
    return {"info": info, "checks": checks, "passed": passed}


# ----------------------------------------------------------------------------
def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--base", default=DEFAULT_BASE,
                    help="корень данных (по умолчанию $HDFS_BASE_DIR, как в w2_etl_formats.py)")
    ap.add_argument("--out-dir", default=str(ROOT / "results" / "raw_metrics"))
    ap.add_argument("--hdfs-cmd", default="hdfs", help="команда hdfs (по умолчанию 'hdfs')")
    ap.add_argument("--expected-rows", type=int, default=None,
                    help="эталонное число строк store_sales (по умолчанию из T11)")
    ap.add_argument("--csv-header", choices=["true", "false"], default=CSV_HEADER_DEFAULT,
                    help="как писался CSV в w2_etl_formats.py (там header=true)")
    ap.add_argument("--csv-sep", default=CSV_SEP_DEFAULT,
                    help="разделитель CSV (в w2_etl_formats.py не задан -> ',')")
    ap.add_argument("--expected-columns", type=int, default=STORE_SALES_COLUMNS,
                    help="число колонок store_sales (0 - не проверять)")
    ap.add_argument("--skip-content", action="store_true",
                    help="не читать содержимое csv_gz/large (только размеры и структура)")
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir)
    expected_rows = args.expected_rows if args.expected_rows is not None else reference_row_count(ROOT)
    fs = make_fs(args.base, args.hdfs_cmd)
    print(f"\nБаза: {args.base}  (файловая система: {fs.name})")

    summary, per_file = [], []
    try:
        for fmt in FORMATS:
            for layout in LAYOUTS:
                path = variant_path(args.base, layout, fmt)
                row, files = summarize(fs, fmt, layout, path)
                summary.append(row)
                per_file += [{"layout": layout, "format": fmt, "file": f["rel"], "kind": f["kind"],
                              "size_bytes": f["size_bytes"]} for f in files]

        plain = {r["layout"]: r["data_bytes"] for r in summary if r["format"] == "csv_plain" and r["status"] == "OK"}
        for r in summary:
            if r["status"] == "OK" and plain.get(r["layout"]):
                r["ratio_vs_csv_plain"] = round(r["data_bytes"] / plain[r["layout"]], 4)

        result = check_csv_gz_large(
            fs, variant_path(args.base, "large", "csv_gz"), plain.get("large"), expected_rows,
            args.csv_header, args.csv_sep, args.expected_columns, args.skip_content)
    except FsError as exc:
        print(f"[ОШИБКА] {exc}")
        return 2

    write_csv(out_dir / "file_stats_summary.csv", summary, [
        "layout", "format", "status", "data_files", "expected_data_files", "files_count_ok",
        "service_files", "data_bytes", "service_bytes", "dir_bytes", "disk_bytes", "min_file_bytes",
        "median_file_bytes", "mean_file_bytes", "max_file_bytes", "ratio_vs_csv_plain", "path"])
    write_csv(out_dir / "file_stats_files.csv", per_file,
              ["layout", "format", "file", "kind", "size_bytes"])
    (out_dir / "csv_gz_large_check.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    hdr = (f"{'layout':<6} {'формат':<10} {'статус':<8} {'файлов':>6} {'ожид.':>5} {'каталог':>12} "
           f"{'на диске':>12} {'мин':>11} {'медиана':>11} {'макс':>11} {'к csv':>7}")
    print(hdr)
    print("-" * len(hdr))
    for r in summary:
        if r["status"] == "MISSING":
            print(f"{r['layout']:<6} {r['format']:<10} {'НЕТ':<8} (каталог не найден: {r['path']})")
            continue
        ratio = f"{r['ratio_vs_csv_plain']:.3f}" if r["ratio_vs_csv_plain"] != "" else "-"
        flag = "" if r["files_count_ok"] == "yes" else "  <-- число файлов != ожидаемому"
        print(f"{r['layout']:<6} {r['format']:<10} {r['status']:<8} {r['data_files']:>6} "
              f"{r['expected_data_files']:>5} {human(r['dir_bytes']):>12} {human(r['disk_bytes']):>12} "
              f"{human(r['min_file_bytes']):>11} {human(r['median_file_bytes']):>11} "
              f"{human(r['max_file_bytes']):>11} {ratio:>7}{flag}")

    print("\n=== Проверка csv_gz/large ===")
    for c in result["checks"]:
        mark = "INFO" if c["level"] == "info" else ("OK  " if c["ok"] else ("FAIL" if c["level"] == "fail" else "WARN"))
        print(f"[{mark}] {c['check']}: {c['detail']}")
    i = result["info"]
    if "uncompressed_bytes" in i:
        print(f"\nСжатый: {i['compressed_human']}, распакованный: {i['uncompressed_human']}, "
              f"сжатие {i['compression_ratio']}x, строк в файле: {i['lines']}, "
              f"из них заголовок: {i['header_lines']}, строк данных: {i['data_rows']}")
    print(f"\nИтог проверки csv_gz/large: {'ПРОЙДЕНА' if result['passed'] else 'НЕ ПРОЙДЕНА'}")
    print(f"Файлы результатов: {out_dir}/file_stats_summary.csv, file_stats_files.csv, csv_gz_large_check.json")

    missing = [r for r in summary if r["status"] != "OK"]
    bad_count = [r for r in summary if r["status"] == "OK" and r["files_count_ok"] != "yes"]
    if missing:
        print(f"\nВнимание: не найдено/пусто вариантов: {len(missing)}")
    if bad_count:
        print(f"Внимание: число файлов отличается от ожидаемого у {len(bad_count)} вариантов")
    return 0 if result["passed"] and not missing else 1


if __name__ == "__main__":
    sys.exit(main())
