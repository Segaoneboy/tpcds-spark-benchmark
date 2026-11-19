#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "results" / "raw_metrics"
PLANS_DIR = ROOT / "results" / "explain_plans"
FINAL_DIR = ROOT / "results" / "final"
CHARTS_DIR = ROOT / "results" / "charts"

FINAL_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "figure.figsize": (11, 6),
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 11,
})

FMT_ORDER = ["parquet", "orc", "avro", "csv_plain", "csv_gz"]
FMT_LABELS = {
    "parquet": "Parquet\n(Snappy)",
    "orc": "ORC\n(Snappy)",
    "avro": "Avro\n(Snappy)",
    "csv_plain": "CSV\n(Plain)",
    "csv_gz": "CSV.GZ\n(Gzip)",
}
COLORS = {"small": "#2563eb", "large": "#ea580c"}

df_write = pd.read_csv(RAW_DIR / "write_metrics.csv")
df_read_raw = pd.read_csv(RAW_DIR / "read_metrics_raw.csv")
df_read_sum = pd.read_csv(RAW_DIR / "read_metrics_summary.csv")

# T58: Сводный results.csv
df_results = df_read_raw.merge(
    df_write[["layout", "format", "codec", "write_time_sec", "file_count", "size_bytes", "size_mb"]],
    on=["layout", "format"],
    how="left",
)
df_results.to_csv(FINAL_DIR / "results.csv", index=False)

# T59: Сводная статистическая таблица
q1 = df_read_sum[df_read_sum["query"] == "Q1_full_scan"].copy()
stats_df = df_write.merge(
    q1[["layout", "format", "source_partitions", "min_sec", "median_sec", "mean_sec", "max_sec", "std_sec"]],
    on=["layout", "format"],
    how="left",
).rename(columns={"median_sec": "q1_median_sec", "mean_sec": "q1_mean_sec", "std_sec": "q1_std_sec"})

base_size = float(stats_df.loc[(stats_df["layout"] == "large") & (stats_df["format"] == "csv_plain"), "size_mb"].iloc[0])
base_q1 = float(stats_df.loc[(stats_df["layout"] == "large") & (stats_df["format"] == "csv_plain"), "q1_median_sec"].iloc[0])

stats_df["compression_ratio_vs_csv"] = (base_size / stats_df["size_mb"]).round(2)
stats_df["q1_speedup_vs_large_csv"] = (base_q1 / stats_df["q1_median_sec"]).round(2)
stats_df.to_csv(FINAL_DIR / "t59_summary_statistics.csv", index=False)

print("=== T59: Сводные метрики ===")
print(stats_df[["layout", "format", "file_count", "size_mb", "compression_ratio_vs_csv", "write_time_sec", "source_partitions", "q1_median_sec", "q1_speedup_vs_large_csv"]].to_string(index=False))

x = range(len(FMT_ORDER))
w = 0.36

# График 1 (T60): Объём хранения
fig, ax = plt.subplots()
for i, layout in enumerate(["small", "large"]):
    sub = df_write[df_write["layout"] == layout].set_index("format").reindex(FMT_ORDER)
    bars = ax.bar([p + (i - 0.5) * w for p in x], sub["size_mb"], width=w,
                  label=f"{layout} ({'16 партиций' if layout=='small' else '1 файл'})", color=COLORS[layout])
    for b in bars:
        ax.annotate(f"{b.get_height():.0f}", (b.get_x() + b.get_width()/2, b.get_height()),
                    ha="center", va="bottom", fontsize=9)
ax.set_xticks(list(x))
ax.set_xticklabels([FMT_LABELS[f] for f in FMT_ORDER])
ax.set_ylabel("Физический объём в HDFS, МиБ")
ax.set_title("T60: Объём хранения таблицы store_sales (14.4 млн строк)")
ax.legend()
plt.tight_layout()
plt.savefig(CHARTS_DIR / "01_storage_size_mb.png", dpi=300)
plt.close()

# График 2 (T60): Время записи ETL
fig, ax = plt.subplots()
for i, layout in enumerate(["small", "large"]):
    sub = df_write[df_write["layout"] == layout].set_index("format").reindex(FMT_ORDER)
    bars = ax.bar([p + (i - 0.5) * w for p in x], sub["write_time_sec"], width=w, label=layout, color=COLORS[layout])
    for b in bars:
        ax.annotate(f"{b.get_height():.1f}с", (b.get_x() + b.get_width()/2, b.get_height()),
                    ha="center", va="bottom", fontsize=9)
ax.set_xticks(list(x))
ax.set_xticklabels([FMT_LABELS[f] for f in FMT_ORDER])
ax.set_ylabel("Время записи ETL, сек")
ax.set_title("T60: Время конвертации и записи в HDFS (W2)")
ax.legend()
plt.tight_layout()
plt.savefig(CHARTS_DIR / "02_write_time_sec.png", dpi=300)
plt.close()

# График 3 (T60 + T61): Q1 Full Scan (small vs large)
fig, ax = plt.subplots()
for i, layout in enumerate(["small", "large"]):
    sub = q1[q1["layout"] == layout].set_index("format").reindex(FMT_ORDER)
    yerr = [sub["median_sec"] - sub["min_sec"], sub["max_sec"] - sub["median_sec"]]
    bars = ax.bar([p + (i - 0.5) * w for p in x], sub["median_sec"], yerr=yerr, capsize=4,
                  width=w, label=layout, color=COLORS[layout])
    for b, sp in zip(bars, sub["source_partitions"]):
        ax.annotate(f"{b.get_height():.1f}с\n({int(sp)}p)", (b.get_x() + b.get_width()/2, b.get_height() + 1.3),
                    ha="center", va="bottom", fontsize=8.5)
ax.set_ylim(0, max(q1["max_sec"]) * 1.18)
ax.set_xticks(list(x))
ax.set_xticklabels([FMT_LABELS[f] for f in FMT_ORDER])
ax.set_ylabel("Медианное время выполнения Q1, сек")
ax.set_title("T61: Q1 Full Scan — влияние раскладки small vs large и числа Spark partitions")
ax.legend()
plt.tight_layout()
plt.savefig(CHARTS_DIR / "03_q1_small_vs_large.png", dpi=300)
plt.close()

# График 4 (T62): Non-splittable эффект
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 5))
sub_large = q1[q1["layout"] == "large"].set_index("format").reindex(FMT_ORDER)
bar_colors = ["#10b981", "#10b981", "#10b981", "#2563eb", "#dc2626"]

b1 = ax1.bar([FMT_LABELS[f] for f in FMT_ORDER], sub_large["source_partitions"], color=bar_colors)
for b in b1:
    ax1.annotate(f"{int(b.get_height())}", (b.get_x() + b.get_width()/2, b.get_height()),
                 ha="center", va="bottom", fontsize=10, fontweight="bold")
ax1.set_ylabel("Входные Spark Partitions (tasks)")
ax1.set_title("Параллелизм чтения (раскладка LARGE)")

b2 = ax2.bar([FMT_LABELS[f] for f in FMT_ORDER], sub_large["median_sec"], color=bar_colors)
for b in b2:
    ax2.annotate(f"{b.get_height():.1f}с", (b.get_x() + b.get_width()/2, b.get_height()),
                 ha="center", va="bottom", fontsize=10, fontweight="bold")
ax2.set_ylabel("Медианное время Q1, сек")
ax2.set_title("Время чтения (штраф 1 потока Gzip)")

plt.suptitle("T62: Эффект нерасщепляемости (non-splittable) сжатого Gzip в HDFS", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig(CHARTS_DIR / "04_nonsplittable_effect.png", dpi=300)
plt.close()

# График 5 (T60): Сравнение Q2 и Q3
fig, ax = plt.subplots()
q2 = df_read_sum[(df_read_sum["query"] == "Q2_selective") & (df_read_sum["layout"] == "large")].set_index("format").reindex(FMT_ORDER)
q3 = df_read_sum[(df_read_sum["query"] == "Q3_join_item") & (df_read_sum["layout"] == "large")].set_index("format").reindex(FMT_ORDER)

b1 = ax.bar([p - w/2 for p in x], q2["median_sec"], width=w, label="Q2: Selective Read (Projection + Filter)", color="#6366f1")
b2 = ax.bar([p + w/2 for p in x], q3["median_sec"], width=w, label="Q3: Join store_sales ⋈ item + Group By", color="#0d9488")
for b in list(b1) + list(b2):
    ax.annotate(f"{b.get_height():.2f}с", (b.get_x() + b.get_width()/2, b.get_height()),
                ha="center", va="bottom", fontsize=8.5)
ax.set_xticks(list(x))
ax.set_xticklabels([FMT_LABELS[f] for f in FMT_ORDER])
ax.set_ylabel("Медианное время, сек (warm OS page cache)")
ax.set_title("T60: Сравнение форматов (large) на запросах Q2 и Q3")
ax.legend()
plt.tight_layout()
plt.savefig(CHARTS_DIR / "05_q2_q3_comparison.png", dpi=300)
plt.close()

# T63: Автоматическая выжимка ключевых операторов EXPLAIN FORMATTED
explain_lines = ["# T63: Сводный анализ физических планов EXPLAIN FORMATTED\n"]
for fmt in FMT_ORDER:
    p_file = PLANS_DIR / f"explain_large_{fmt}.txt"
    if p_file.exists():
        explain_lines.append(f"## Формат: large/{fmt}")
        for line in p_file.read_text(encoding="utf-8").splitlines():
            if any(k in line for k in ["===", "Scan ", "ReadSchema:", "PushedFilters:", "BroadcastHashJoin"]):
                explain_lines.append(f"- `{line.strip()}`")
        explain_lines.append("")
(FINAL_DIR / "t63_explain_analysis.md").write_text("\n".join(explain_lines), encoding="utf-8")

print("\n>>> Сгенерированы файлы W4:")
for f in sorted(FINAL_DIR.glob("*")) + sorted(CHARTS_DIR.glob("*.png")):
    print("  -", f.relative_to(ROOT))
