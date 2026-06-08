"""
Cross-system comparison: Edge Impulse vs Zephyr TFLite on nRF5340.

Loads PPK2 power traces and inference logs from both builds and generates
side-by-side comparison plots saved to finalproject/plots/.

Usage:
    python compare.py [--vdd 3.0]
"""

import argparse
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

HERE = Path(__file__).parent
EI_PPK    = HERE / "edge_impulse/logs/ppk-20260530T045430.csv"
EI_EVENTS = HERE / "edge_impulse/logs/ppk-20260530T045430_events.csv"
EI_LOG    = HERE / "edge_impulse/logs/1050030941 (RTT).log"
ZT_PPK    = HERE / "zephyr_tflite/logs/ppk-20260608T000110.csv"
ZT_LOG    = HERE / "zephyr_tflite/logs/serial-terminal-07062026_161522.txt"

COLORS = {"ei": "#1f77b4", "zt": "#d62728"}
LABELS = {"ei": "Edge Impulse", "zt": "Zephyr TFLite"}


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_ppk(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if "timestamp" in cl or cl.startswith("time"):
            rename[c] = "t_ms"
        elif "current" in cl:
            rename[c] = "i_ua"
    df = df.rename(columns=rename)
    return df[["t_ms", "i_ua"]].copy()


def load_ei_events(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def load_zt_rows(path: Path):
    ROW_RE = re.compile(
        r"x:\s*(-?\d+\.\d+)\s+pred:\s*(-?\d+\.\d+)\s+true:\s*(-?\d+\.\d+)\s+err:\s*(-?\d+\.\d+)"
    )
    rows = []
    with path.open() as f:
        for line in f:
            m = ROW_RE.search(line)
            if m:
                rows.append({
                    "x": float(m.group(1)),
                    "pred": float(m.group(2)),
                    "true": float(m.group(3)),
                    "err": float(m.group(4)),
                })
    return rows


TS_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.(\d{3}),(\d{3})\]")
ML_RE = re.compile(
    r"e:ml_result_event\s+(\S+)\s+val:\s*(-?\d+\.\d+)\s+anomaly:\s*(-?\d+\.\d+)"
)

def load_ei_log(path: Path):
    def ts(m):
        h, mn, s, ms, us = map(int, m.groups())
        return h * 3600 + mn * 60 + s + ms / 1e3 + us / 1e6
    events = []
    with path.open() as f:
        for line in f:
            ts_m = TS_RE.search(line)
            ml_m = ML_RE.search(line)
            if ts_m and ml_m:
                events.append({
                    "t": ts(ts_m),
                    "val": float(ml_m.group(2)),
                    "anomaly": float(ml_m.group(3)),
                })
    return events


# ---------------------------------------------------------------------------
# Plot 1: Side-by-side current traces
# ---------------------------------------------------------------------------

def plot_traces(ei_ppk, zt_ppk, out_path: Path):
    fig, axes = plt.subplots(2, 1, figsize=(14, 6), sharex=False)

    for ax, df, key in zip(axes, [ei_ppk, zt_ppk], ["ei", "zt"]):
        t_s = (df["t_ms"].values - df["t_ms"].values[0]) / 1000.0
        i_ma = df["i_ua"].values / 1000.0
        mean_ma = i_ma.mean()
        ax.plot(t_s, i_ma, color=COLORS[key], linewidth=0.5, alpha=0.8)
        ax.axhline(mean_ma, color="black", linestyle="--", linewidth=1.0,
                   label=f"mean = {mean_ma*1000:.0f} µA")
        ax.set_ylabel("Current (mA)")
        ax.set_title(f"{LABELS[key]}", fontsize=11, fontweight="bold",
                     color=COLORS[key])
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, t_s[-1])

    axes[-1].set_xlabel("Time since capture start (s)")
    fig.suptitle("Current traces: Edge Impulse vs Zephyr TFLite",
                 fontsize=13, fontweight="bold", y=1.01)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot 2: Current distribution overlay
# ---------------------------------------------------------------------------

def plot_current_dist_overlay(ei_ppk, zt_ppk, out_path: Path):
    fig, ax = plt.subplots(figsize=(9, 4))

    for df, key in [(ei_ppk, "ei"), (zt_ppk, "zt")]:
        i_ua = df["i_ua"].values
        ax.hist(i_ua, bins=80, color=COLORS[key], alpha=0.55,
                edgecolor="none", label=f"{LABELS[key]}  (mean {i_ua.mean():.0f} µA)",
                density=True)

    ax.set_xlabel("Current (µA)")
    ax.set_ylabel("Density")
    ax.set_title("Current distribution overlay", fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot 3: Power metrics bar chart
# ---------------------------------------------------------------------------

def plot_power_metrics(ei_ppk, zt_ppk, vdd_v: float, out_path: Path):
    metrics = {}
    for df, key in [(ei_ppk, "ei"), (zt_ppk, "zt")]:
        i = df["i_ua"].values
        metrics[key] = {
            "baseline": float(np.median(i[i <= np.percentile(i, 30)])),
            "mean": float(i.mean()),
            "peak": float(i.max()),
        }

    categories = ["Baseline", "Mean", "Peak"]
    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))
    for i, (key, offset) in enumerate(zip(["ei", "zt"], [-width/2, width/2])):
        vals = [metrics[key]["baseline"] * vdd_v / 1000,
                metrics[key]["mean"] * vdd_v / 1000,
                metrics[key]["peak"] * vdd_v / 1000]
        bars = ax.bar(x + offset, vals, width, label=LABELS[key],
                      color=COLORS[key], alpha=0.8, edgecolor="black")
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(v for vv in [
                        [metrics[k]["peak"]*vdd_v/1000 for k in ["ei","zt"]]
                    ] for v in vv) * 0.02,
                    f"{v:.1f}", ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel("Power (mW)")
    ax.set_title(f"Power comparison at Vdd = {vdd_v} V", fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot 4: Battery life comparison
# ---------------------------------------------------------------------------

def plot_battery_life(ei_ppk, zt_ppk, out_path: Path):
    batteries = [("CR2032\n(225 mAh)", 225),
                 ("AAA\n(1200 mAh)", 1200),
                 ("AA\n(2500 mAh)", 2500)]

    ei_mean = ei_ppk["i_ua"].mean()
    zt_mean = zt_ppk["i_ua"].mean()

    x = np.arange(len(batteries))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))

    for mean_ua, key, offset in [(ei_mean, "ei", -width/2), (zt_mean, "zt", width/2)]:
        vals = [cap * 1000 / mean_ua / 24 for _, cap in batteries]  # days
        bars = ax.bar(x + offset, vals, width, label=LABELS[key],
                      color=COLORS[key], alpha=0.8, edgecolor="black")
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() * 1.02,
                    f"{v:.1f}d", ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([label for label, _ in batteries])
    ax.set_ylabel("Estimated battery life (days)")
    ax.set_title("Battery life projections at current duty cycle", fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot 5: Inference performance comparison
# ---------------------------------------------------------------------------

def plot_inference_perf(ei_events, ei_log, zt_rows, zt_ppk, vdd_v: float, out_path: Path):
    inf = ei_events[ei_events["type"] == "inference"]
    ei_duration_s = (inf["t_start_ms"].max() - inf["t_start_ms"].min()) / 1000.0
    ei_rate = len(inf) / ei_duration_s
    ei_avg_dur_ms = inf["duration_ms"].mean()
    ei_energy_uj = inf["energy_marginal_uj"].mean()

    zt_duration_s = (zt_ppk["t_ms"].iloc[-1] - zt_ppk["t_ms"].iloc[0]) / 1000.0
    zt_n = len([r for r in zt_rows if r["x"] != 0.0])
    zt_rate = zt_n / zt_duration_s
    zt_avg_dur_ms = zt_duration_s * 1000 / zt_n
    zt_mean_ua = zt_ppk["i_ua"].mean()
    zt_energy_uj = zt_mean_ua * (zt_avg_dur_ms / 1000) * vdd_v  # total (no idle state)

    fig, axes = plt.subplots(1, 3, figsize=(13, 4))

    # Inference rate
    ax = axes[0]
    bars = ax.bar(["EI", "ZT"], [ei_rate, zt_rate],
                  color=[COLORS["ei"], COLORS["zt"]], alpha=0.8, edgecolor="black", width=0.5)
    for bar, v in zip(bars, [ei_rate, zt_rate]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.02,
                f"{v:.1f}/s", ha="center", fontsize=10, fontweight="bold")
    ax.set_ylabel("Inferences / second")
    ax.set_title("Inference rate", fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    # Average inference duration
    ax = axes[1]
    bars = ax.bar(["EI", "ZT"], [ei_avg_dur_ms, zt_avg_dur_ms],
                  color=[COLORS["ei"], COLORS["zt"]], alpha=0.8, edgecolor="black", width=0.5)
    for bar, v in zip(bars, [ei_avg_dur_ms, zt_avg_dur_ms]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.02,
                f"{v:.1f} ms", ha="center", fontsize=10, fontweight="bold")
    ax.set_ylabel("Duration (ms)")
    ax.set_title("Avg time per inference", fontweight="bold")
    ax.grid(True, alpha=0.3, axis="y")

    # Energy per inference
    ax = axes[2]
    bars = ax.bar(["EI\n(marginal)", "ZT\n(total*)"], [ei_energy_uj, zt_energy_uj],
                  color=[COLORS["ei"], COLORS["zt"]], alpha=0.8, edgecolor="black", width=0.5)
    for bar, v in zip(bars, [ei_energy_uj, zt_energy_uj]):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height()*1.02,
                f"{v:.1f} µJ", ha="center", fontsize=10, fontweight="bold")
    ax.set_ylabel("Energy per inference (µJ)")
    ax.set_title("Energy per inference", fontweight="bold")
    ax.annotate("* ZT has no idle state;\n  total energy is shown",
                xy=(0.98, 0.05), xycoords="axes fraction",
                ha="right", fontsize=7, color="gray")
    ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Inference performance: Edge Impulse vs Zephyr TFLite",
                 fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Plot 6: Inference accuracy (EI confidence vs ZT absolute error)
# ---------------------------------------------------------------------------

def plot_inference_accuracy(ei_log, zt_rows, out_path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))

    # EI: classification confidence over time
    ax = axes[0]
    if ei_log:
        t = np.array([e["t"] for e in ei_log])
        val = np.array([e["val"] for e in ei_log])
        ax.plot(t - t[0], val, "o-", color=COLORS["ei"], markersize=3,
                linewidth=1.0, alpha=0.8)
        ax.axhline(val.mean(), color="black", linestyle="--", linewidth=1,
                   label=f"mean = {val.mean():.3f}")
        ax.set_ylim(0, 1.05)
        ax.set_xlabel("Time since first inference (s)")
        ax.set_ylabel("Classification confidence")
        ax.set_title("Edge Impulse\nClassification confidence over time",
                     fontsize=10, fontweight="bold", color=COLORS["ei"])
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "No EI inference log found", ha="center", va="center",
                transform=ax.transAxes)

    # ZT: absolute error distribution
    ax = axes[1]
    active = [r for r in zt_rows if r["x"] != 0.0]
    errs = np.abs([r["err"] for r in active])
    ax.hist(errs, bins=20, color=COLORS["zt"], alpha=0.75, edgecolor="black",
            density=True)
    ax.axvline(errs.mean(), color="black", linestyle="--", linewidth=1.5,
               label=f"MAE = {errs.mean():.4f}")
    ax.set_xlabel("|prediction error|  (pred − true)")
    ax.set_ylabel("Density")
    ax.set_title("Zephyr TFLite\nAbsolute error distribution (sine regression)",
                 fontsize=10, fontweight="bold", color=COLORS["zt"])
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    fig.suptitle("Inference accuracy comparison\n"
                 "(different tasks: classification vs regression)",
                 fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Summary printout
# ---------------------------------------------------------------------------

def print_comparison(ei_ppk, zt_ppk, ei_events, zt_rows, vdd_v):
    ei_i = ei_ppk["i_ua"].values
    zt_i = zt_ppk["i_ua"].values
    ei_baseline = float(np.median(ei_i[ei_i <= np.percentile(ei_i, 30)]))
    inf = ei_events[ei_events["type"] == "inference"]
    ble = ei_events[ei_events["type"] == "ble"]
    ei_dur = (inf["t_start_ms"].max() - inf["t_start_ms"].min()) / 1000.0
    zt_dur = (zt_ppk["t_ms"].iloc[-1] - zt_ppk["t_ms"].iloc[0]) / 1000.0
    zt_active = [r for r in zt_rows if r["x"] != 0.0]
    zt_errs = np.abs([r["err"] for r in zt_active])

    print("=" * 68)
    print(f"{'METRIC':<34} {'EDGE IMPULSE':>15} {'ZEPHYR TFLITE':>15}")
    print("=" * 68)
    print(f"{'Capture duration (s)':<34} {ei_dur:>15.2f} {zt_dur:>15.3f}")
    print(f"{'Baseline current (µA)':<34} {ei_baseline:>15.1f} {'N/A (no idle)':>15}")
    print(f"{'Mean current (µA)':<34} {ei_i.mean():>15.1f} {zt_i.mean():>15.1f}")
    print(f"{'Peak current (µA)':<34} {ei_i.max():>15.1f} {zt_i.max():>15.1f}")
    print(f"{'Avg power (mW)':<34} {ei_i.mean()*vdd_v/1e3:>15.2f} {zt_i.mean()*vdd_v/1e3:>15.2f}")
    print(f"{'Inference events':<34} {len(inf):>15} {len(zt_active):>15}")
    print(f"{'BLE radio bursts':<34} {len(ble):>15} {'0 (no BLE)':>15}")
    print(f"{'Inference rate (inf/s)':<34} {len(inf)/ei_dur:>15.1f} {len(zt_active)/zt_dur:>15.1f}")
    print(f"{'Avg inference duration (ms)':<34} {inf['duration_ms'].mean():>15.1f} {zt_dur*1000/len(zt_active):>15.1f}")
    zt_energy = zt_i.mean() * (zt_dur / len(zt_active)) * vdd_v
    print(f"{'Energy/inference — EI marginal (µJ)':<34} {inf['energy_marginal_uj'].mean():>15.2f} {'':>15}")
    print(f"{'Energy/inference — total (µJ)':<34} {'':>15} {zt_energy:>15.2f}")
    print(f"{'MAE (regression)':<34} {'N/A':>15} {zt_errs.mean():>15.4f}")
    print(f"{'RMSE (regression)':<34} {'N/A':>15} {np.sqrt(np.mean(np.array([r['err'] for r in zt_active])**2)):>15.4f}")
    print("-" * 68)
    print("Battery life at mean current draw:")
    for cell, mAh in [("  CR2032 (225 mAh)", 225), ("  AAA (1200 mAh)", 1200), ("  AA (2500 mAh)", 2500)]:
        ei_days = mAh * 1000 / ei_i.mean() / 24
        zt_days = mAh * 1000 / zt_i.mean() / 24
        print(f"  {cell:<30} {ei_days:>12.1f} d {zt_days:>12.1f} d")
    print("=" * 68)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vdd", type=float, default=3.0)
    args = parser.parse_args()

    for p in [EI_PPK, EI_EVENTS, ZT_PPK, ZT_LOG]:
        if not p.exists():
            raise SystemExit(f"Required file not found: {p}")

    ei_ppk    = load_ppk(EI_PPK)
    zt_ppk    = load_ppk(ZT_PPK)
    ei_events = load_ei_events(EI_EVENTS)
    zt_rows   = load_zt_rows(ZT_LOG)
    ei_log    = load_ei_log(EI_LOG) if EI_LOG.exists() else []

    print_comparison(ei_ppk, zt_ppk, ei_events, zt_rows, args.vdd)

    out = HERE / "plots"
    out.mkdir(exist_ok=True)

    plot_traces(ei_ppk, zt_ppk, out / "comparison_traces.png")
    plot_current_dist_overlay(ei_ppk, zt_ppk, out / "comparison_current_dist.png")
    plot_power_metrics(ei_ppk, zt_ppk, args.vdd, out / "comparison_power_metrics.png")
    plot_battery_life(ei_ppk, zt_ppk, out / "comparison_battery_life.png")
    plot_inference_perf(ei_events, ei_log, zt_rows, zt_ppk, args.vdd,
                        out / "comparison_inference_perf.png")
    plot_inference_accuracy(ei_log, zt_rows, out / "comparison_accuracy.png")


if __name__ == "__main__":
    main()
