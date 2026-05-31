"""
Nordic Power Profiler Kit II (PPK2) trace analyzer.

Reads a PPK2-format CSV (columns: Timestamp(ms), Current(uA), D0-D7) and produces:
  - Event detection (small inference bursts vs large BLE/radio bursts)
  - Per-event energy in microjoules
  - Average power and battery-life estimate
  - Separate figures for each panel of the analysis

Usage:
    python plot_ppk.py [path/to/trace.csv] [--vdd 3.0]

Assumptions worth noting in any report:
  - Supply voltage Vdd defaults to 3.0 V (PPK2 default source mode, typical for
    nRF5340 development). Pass --vdd to override.
  - The PPK2 samples at 1 kHz when exported (the high-resolution mode buffers
    are downsampled to 1 ms on export). All energy integrals assume dt = 1 ms.
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.signal import find_peaks


# ---------------------------------------------------------------------------
# Loading & cleanup
# ---------------------------------------------------------------------------

def load_ppk2_csv(path: Path) -> pd.DataFrame:
    """Load a PPK2 export. Tolerant of column-name variations."""
    df = pd.read_csv(path)
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if "timestamp" in cl or cl.startswith("time"):
            rename[c] = "t_ms"
        elif "current" in cl:
            rename[c] = "i_ua"
    df = df.rename(columns=rename)
    if "t_ms" not in df or "i_ua" not in df:
        raise SystemExit(f"Could not find timestamp/current columns. Got: {list(df.columns)}")
    return df[["t_ms", "i_ua"]].copy()


# ---------------------------------------------------------------------------
# Event detection
# ---------------------------------------------------------------------------

def estimate_baseline(i_ua: np.ndarray) -> float:
    """Baseline = median of the lower 30% of samples (robust to bursts)."""
    threshold = np.percentile(i_ua, 30)
    return float(np.median(i_ua[i_ua <= threshold]))


def find_bursts(i_ua: np.ndarray, baseline: float, threshold_ua: float = 150.0,
                min_duration_ms: int = 2, merge_gap_ms: int = 2):
    """
    Identify contiguous bursts where current is sustained above baseline + threshold.
    Returns list of (start_idx, end_idx) inclusive on start, exclusive on end.
    Short gaps between bursts (< merge_gap_ms) are merged.
    """
    above = i_ua > (baseline + threshold_ua)
    edges = np.diff(above.astype(int))
    starts = np.where(edges == 1)[0] + 1
    ends = np.where(edges == -1)[0] + 1
    if above[0]:
        starts = np.insert(starts, 0, 0)
    if above[-1]:
        ends = np.append(ends, len(i_ua))

    bursts = list(zip(starts.tolist(), ends.tolist()))

    merged = []
    for s, e in bursts:
        if merged and s - merged[-1][1] < merge_gap_ms:
            merged[-1] = (merged[-1][0], e)
        else:
            merged.append((s, e))

    merged = [(s, e) for s, e in merged if e - s >= min_duration_ms]
    return merged


def classify_burst(peak_ua: float, ble_threshold_ua: float = 5000.0) -> str:
    return "ble" if peak_ua > ble_threshold_ua else "inference"


# ---------------------------------------------------------------------------
# Energy calculations
# ---------------------------------------------------------------------------

def burst_energies(t_ms: np.ndarray, i_ua: np.ndarray, bursts, baseline_ua: float,
                   vdd_v: float):
    """
    For each burst, compute total energy and marginal energy (above baseline).

    Energy: E [uJ] = V [V] * integral( I [uA] * dt [s] ) / 1e6 (uA->A)
                   = V * sum( I_uA * 0.001 ) / 1e6  for dt = 1 ms
                   = V * sum(I_uA) * 1e-9  [J] = V * sum(I_uA) * 1e-3 [uJ]
    """
    rows = []
    for s, e in bursts:
        segment = i_ua[s:e]
        peak = float(segment.max())
        mean = float(segment.mean())
        duration_ms = e - s
        e_total_uj = vdd_v * segment.sum() * 1e-3
        e_marginal_uj = vdd_v * (segment - baseline_ua).sum() * 1e-3
        rows.append({
            "t_start_ms": float(t_ms[s]),
            "t_end_ms": float(t_ms[e - 1]),
            "duration_ms": duration_ms,
            "peak_ua": peak,
            "mean_ua": mean,
            "energy_total_uj": e_total_uj,
            "energy_marginal_uj": e_marginal_uj,
            "type": classify_burst(peak),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Plotting — one figure per panel
# ---------------------------------------------------------------------------

def plot_full_trace(df_trace, df_events, baseline_ua, mean_current_ua, out_path: Path):
    from matplotlib.patches import Patch
    t_s = df_trace["t_ms"].values / 1000.0
    i_ma = df_trace["i_ua"].values / 1000.0

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(t_s, i_ma, color="#444", linewidth=0.5)
    ax.axhline(baseline_ua / 1000, color="green", linestyle="--",
               linewidth=1, label=f"baseline = {baseline_ua:.0f} µA")
    ax.axhline(mean_current_ua / 1000, color="orange", linestyle=":",
               linewidth=1.2, label=f"mean = {mean_current_ua:.0f} µA")

    inf_events = df_events[df_events["type"] == "inference"]
    ble_events = df_events[df_events["type"] == "ble"]
    for _, ev in inf_events.iterrows():
        ax.axvspan(ev["t_start_ms"] / 1000, ev["t_end_ms"] / 1000,
                   color="#1f77b4", alpha=0.15)
    for _, ev in ble_events.iterrows():
        ax.axvspan(ev["t_start_ms"] / 1000, ev["t_end_ms"] / 1000,
                   color="#d62728", alpha=0.20)

    handles, labels = ax.get_legend_handles_labels()
    handles += [Patch(facecolor="#1f77b4", alpha=0.4, label="inference burst"),
                Patch(facecolor="#d62728", alpha=0.4, label="BLE radio burst")]
    ax.legend(handles=handles, loc="upper right", fontsize=9, ncol=2)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Current (mA)")
    ax.set_title("Full current trace with detected events",
                 fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.set_xlim(t_s[0], t_s[-1])

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure: {out_path}")
    plt.close(fig)


def plot_inference_zoom(df_trace, df_events, out_path: Path):
    inf_events = df_events[df_events["type"] == "inference"]
    fig, ax = plt.subplots(figsize=(7, 4))
    if len(inf_events) > 0:
        ev = inf_events.iloc[len(inf_events) // 2]
        pad = 8
        mask = (df_trace["t_ms"] >= ev["t_start_ms"] - pad) & \
               (df_trace["t_ms"] <= ev["t_end_ms"] + pad)
        ax.plot(df_trace.loc[mask, "t_ms"], df_trace.loc[mask, "i_ua"],
                "o-", color="#1f77b4", markersize=3)
        ax.axvspan(ev["t_start_ms"], ev["t_end_ms"], color="#1f77b4", alpha=0.15)
        ax.set_title(f"Zoom: inference burst\n"
                     f"duration {ev['duration_ms']} ms, "
                     f"peak {ev['peak_ua']:.0f} µA, "
                     f"E={ev['energy_marginal_uj']:.2f} µJ",
                     fontsize=10, fontweight="bold")
    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("Current (µA)")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure: {out_path}")
    plt.close(fig)


def plot_ble_zoom(df_trace, df_events, out_path: Path):
    ble_events = df_events[df_events["type"] == "ble"]
    fig, ax = plt.subplots(figsize=(7, 4))
    if len(ble_events) > 0:
        ev = ble_events.iloc[len(ble_events) // 2]
        pad = 8
        mask = (df_trace["t_ms"] >= ev["t_start_ms"] - pad) & \
               (df_trace["t_ms"] <= ev["t_end_ms"] + pad)
        ax.plot(df_trace.loc[mask, "t_ms"], df_trace.loc[mask, "i_ua"],
                "o-", color="#d62728", markersize=3)
        ax.axvspan(ev["t_start_ms"], ev["t_end_ms"], color="#d62728", alpha=0.20)
        ax.set_title(f"Zoom: BLE radio burst\n"
                     f"duration {ev['duration_ms']} ms, "
                     f"peak {ev['peak_ua']:.0f} µA, "
                     f"E={ev['energy_marginal_uj']:.2f} µJ",
                     fontsize=10, fontweight="bold")
    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("Current (µA)")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure: {out_path}")
    plt.close(fig)


def plot_current_dist(df_trace, baseline_ua, mean_current_ua, out_path: Path):
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(df_trace["i_ua"], bins=100, color="#555", alpha=0.8, edgecolor="none")
    ax.axvline(baseline_ua, color="green", linestyle="--",
               linewidth=1.2, label=f"baseline {baseline_ua:.0f} µA")
    ax.axvline(mean_current_ua, color="orange", linestyle=":",
               linewidth=1.5, label=f"mean {mean_current_ua:.0f} µA")
    ax.set_xlabel("Current (µA)")
    ax.set_ylabel("Sample count (log)")
    ax.set_yscale("log")
    ax.set_title("Current distribution (log scale reveals burst tails)",
                 fontsize=10, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure: {out_path}")
    plt.close(fig)


def plot_energy_per_event(df_events, out_path: Path):
    fig, ax = plt.subplots(figsize=(6, 4))
    if len(df_events) > 0:
        types = ["inference", "ble"]
        colors = {"inference": "#1f77b4", "ble": "#d62728"}
        labels_x, means, stds, bar_colors = [], [], [], []
        for t in types:
            sub = df_events[df_events["type"] == t]["energy_marginal_uj"]
            if len(sub):
                labels_x.append(f"{t}\n(n={len(sub)})")
                means.append(sub.mean())
                stds.append(sub.std())
                bar_colors.append(colors[t])
        x_pos = np.arange(len(labels_x))
        ax.bar(x_pos, means, yerr=stds, capsize=8,
               color=bar_colors, alpha=0.8, edgecolor="black")
        for i, (m, s) in enumerate(zip(means, stds)):
            ax.text(i, m + s + max(means) * 0.05, f"{m:.1f} ± {s:.1f} µJ",
                    ha="center", fontsize=9, fontweight="bold")
        ax.set_xticks(x_pos)
        ax.set_xticklabels(labels_x)
        ax.set_ylabel("Marginal energy per event (µJ)")
        ax.set_title("Energy per event by type (mean ± std)",
                     fontsize=10, fontweight="bold")
        ax.grid(True, alpha=0.3, axis="y")

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure: {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Summary stats printout
# ---------------------------------------------------------------------------

def print_summary(df_trace, df_events, baseline_ua, mean_current_ua, vdd_v):
    duration_s = (df_trace["t_ms"].iloc[-1] - df_trace["t_ms"].iloc[0]) / 1000.0
    total_charge_uc = df_trace["i_ua"].sum() * 1e-3
    total_energy_mj = vdd_v * total_charge_uc * 1e-3
    avg_power_uw = mean_current_ua * vdd_v

    inf_n = (df_events["type"] == "inference").sum()
    ble_n = (df_events["type"] == "ble").sum()

    print("=" * 64)
    print("PPK2 POWER CAPTURE SUMMARY")
    print("=" * 64)
    print(f"Capture duration:           {duration_s:.2f} s")
    print(f"Supply voltage (assumed):   {vdd_v} V")
    print(f"Baseline current:           {baseline_ua:.1f} µA  ({baseline_ua*vdd_v:.0f} µW)")
    print(f"Mean current:               {mean_current_ua:.1f} µA  ({avg_power_uw:.0f} µW)")
    print(f"Peak current:               {df_trace['i_ua'].max():.1f} µA")
    print(f"Total energy used:          {total_energy_mj:.2f} mJ")
    print()
    print("EVENT BREAKDOWN")
    print(f"Inference bursts detected:  {inf_n}")
    if inf_n:
        inf = df_events[df_events["type"] == "inference"]
        print(f"  avg duration:             {inf['duration_ms'].mean():.1f} ms")
        print(f"  avg peak current:         {inf['peak_ua'].mean():.0f} µA")
        print(f"  avg marginal energy:      {inf['energy_marginal_uj'].mean():.2f} µJ")
        print(f"  avg interval between:     {duration_s*1000/inf_n:.1f} ms")
    print(f"BLE radio bursts detected:  {ble_n}")
    if ble_n:
        ble = df_events[df_events["type"] == "ble"]
        print(f"  avg duration:             {ble['duration_ms'].mean():.1f} ms")
        print(f"  avg peak current:         {ble['peak_ua'].mean():.0f} µA")
        print(f"  avg marginal energy:      {ble['energy_marginal_uj'].mean():.2f} µJ")
        print(f"  avg interval between:     {duration_s*1000/ble_n:.1f} ms")
    print()
    print("BATTERY-LIFE PROJECTIONS (at this duty cycle)")
    for cell, mAh in [("CR2032", 225), ("AAA alkaline", 1200), ("AA alkaline", 2500)]:
        hours = mAh * 1000.0 / mean_current_ua
        if hours > 24:
            print(f"  {cell:18s} ({mAh} mAh):  {hours/24:.1f} days  ({hours:.0f} h)")
        else:
            print(f"  {cell:18s} ({mAh} mAh):  {hours:.1f} h")
    print("=" * 64)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PPK2 trace analyzer")
    parser.add_argument("csv", nargs="?", default="power.csv", type=Path)
    parser.add_argument("--vdd", type=float, default=3.0,
                        help="Supply voltage in volts (default 3.0)")
    parser.add_argument("--ble-threshold", type=float, default=5000.0,
                        help="Peak current (µA) above which a burst is classified as BLE radio")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")

    df = load_ppk2_csv(args.csv)
    t_ms = df["t_ms"].values
    i_ua = df["i_ua"].values

    baseline = estimate_baseline(i_ua)
    mean_i = float(i_ua.mean())

    bursts = find_bursts(i_ua, baseline, threshold_ua=150.0,
                         min_duration_ms=2, merge_gap_ms=2)
    df_events = burst_energies(t_ms, i_ua, bursts, baseline, args.vdd)
    df_events["type"] = df_events["peak_ua"].apply(
        lambda p: "ble" if p > args.ble_threshold else "inference"
    )

    print_summary(df, df_events, baseline, mean_i, args.vdd)

    plots_dir = args.csv.parent / "plots"
    plots_dir.mkdir(exist_ok=True)
    stem = args.csv.stem
    plot_full_trace(df, df_events, baseline, mean_i,
                    plots_dir / f"{stem}_trace.png")
    plot_inference_zoom(df, df_events,
                        plots_dir / f"{stem}_inference_zoom.png")
    plot_ble_zoom(df, df_events,
                  plots_dir / f"{stem}_ble_zoom.png")
    plot_current_dist(df, baseline, mean_i,
                      plots_dir / f"{stem}_current_dist.png")
    plot_energy_per_event(df_events,
                          plots_dir / f"{stem}_energy_per_event.png")

    events_csv = args.csv.with_name(args.csv.stem + "_events.csv")
    df_events.to_csv(events_csv, index=False)
    print(f"Saved per-event table: {events_csv}")


if __name__ == "__main__":
    main()
