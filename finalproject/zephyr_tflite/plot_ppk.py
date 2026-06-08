"""
Nordic Power Profiler Kit II (PPK2) trace analyzer — zephyr_tflite build.

Unlike the Edge Impulse build, the zephyr_tflite firmware has no BLE radio and runs
inference in a tight loop, so the current trace is near-constant rather than bursty.
Analysis focuses on steady-state power, micro-variation, and battery-life projections.

Usage:
    python plot_ppk.py [path/to/trace.csv] [--vdd 3.0]
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_ppk2_csv(path: Path) -> pd.DataFrame:
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
# Plotting
# ---------------------------------------------------------------------------

def plot_full_trace(df, mean_ua, out_path: Path):
    t_s = df["t_ms"].values / 1000.0
    i_ma = df["i_ua"].values / 1000.0

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(t_s, i_ma, color="#444", linewidth=0.6, alpha=0.8)
    ax.axhline(mean_ua / 1000, color="orange", linestyle="--",
               linewidth=1.2, label=f"mean = {mean_ua:.0f} µA")

    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Current (mA)")
    ax.set_title("Full current trace — zephyr_tflite (continuous inference, no BLE)",
                 fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(t_s[0], t_s[-1])
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


def plot_rolling_stats(df, window_ms: int, out_path: Path):
    """Rolling mean and std to reveal slow drift or periodic load."""
    i_ua = df["i_ua"].values
    t_s = df["t_ms"].values / 1000.0

    roll_mean = pd.Series(i_ua).rolling(window_ms, center=True).mean().values
    roll_std  = pd.Series(i_ua).rolling(window_ms, center=True).std().values

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(t_s, i_ua / 1000, color="#aaa", linewidth=0.5, alpha=0.6, label="raw (mA)")
    ax.plot(t_s, roll_mean / 1000, color="#1f77b4", linewidth=1.5,
            label=f"rolling mean ({window_ms} ms window)")
    ax.fill_between(
        t_s,
        (roll_mean - roll_std) / 1000,
        (roll_mean + roll_std) / 1000,
        color="#1f77b4", alpha=0.15, label="±1σ",
    )
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Current (mA)")
    ax.set_title(f"Rolling mean ± 1σ  (window = {window_ms} ms)",
                 fontsize=11, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(t_s[0], t_s[-1])
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


def plot_current_dist(df, mean_ua, out_path: Path):
    i_ua = df["i_ua"].values

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(i_ua, bins=60, color="#1f77b4", alpha=0.75, edgecolor="none")
    ax.axvline(mean_ua, color="orange", linestyle="--",
               linewidth=1.5, label=f"mean = {mean_ua:.0f} µA")
    ax.axvline(np.median(i_ua), color="green", linestyle=":",
               linewidth=1.2, label=f"median = {np.median(i_ua):.0f} µA")
    ax.set_xlabel("Current (µA)")
    ax.set_ylabel("Sample count")
    ax.set_title("Current distribution — tight spread indicates steady-state load",
                 fontsize=10, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


def plot_power_spectrum(df, out_path: Path):
    """FFT of the current signal to reveal any periodic inference cadence."""
    i_ua = df["i_ua"].values - df["i_ua"].mean()
    fs_hz = 1000.0  # PPK2 exports at 1 kHz
    n = len(i_ua)
    freqs = np.fft.rfftfreq(n, d=1.0 / fs_hz)
    power = np.abs(np.fft.rfft(i_ua)) ** 2

    # Only plot up to 50 Hz (inference cadence is sub-50 Hz)
    mask = freqs <= 50
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.semilogy(freqs[mask], power[mask], color="#9467bd", linewidth=0.8)
    ax.set_xlabel("Frequency (Hz)")
    ax.set_ylabel("Power spectral density")
    ax.set_title("Power spectrum of current trace (DC removed)\n"
                 "peaks reveal periodic activity (inference loop, clock, etc.)",
                 fontsize=10, fontweight="bold")
    ax.grid(True, alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------

def print_summary(df, mean_ua, vdd_v):
    i_ua = df["i_ua"].values
    duration_s = (df["t_ms"].iloc[-1] - df["t_ms"].iloc[0]) / 1000.0
    total_charge_uc = i_ua.sum() * 1e-3  # sum(µA) * 1e-3 ms/s = µC
    total_energy_mj = vdd_v * total_charge_uc * 1e-3
    avg_power_mw = mean_ua * vdd_v / 1000.0

    print("=" * 64)
    print("PPK2 POWER CAPTURE SUMMARY  (zephyr_tflite)")
    print("=" * 64)
    print(f"Capture duration:           {duration_s:.3f} s  ({len(df)} samples @ 1 kHz)")
    print(f"Supply voltage (assumed):   {vdd_v} V")
    print()
    print("Current (µA):")
    print(f"  min / mean / max:         {i_ua.min():.1f} / {mean_ua:.1f} / {i_ua.max():.1f}")
    print(f"  std (jitter):             {i_ua.std():.2f} µA")
    print(f"  p5 / p95:                 {np.percentile(i_ua,5):.1f} / {np.percentile(i_ua,95):.1f}")
    print()
    print(f"Average power:              {avg_power_mw:.2f} mW  ({mean_ua*vdd_v:.0f} µW)")
    print(f"Total energy in capture:    {total_energy_mj:.2f} mJ")
    print()
    print("BATTERY-LIFE PROJECTIONS (at this duty cycle)")
    for cell, mAh in [("CR2032", 225), ("AAA alkaline", 1200), ("AA alkaline", 2500)]:
        hours = mAh * 1000.0 / mean_ua
        if hours > 24:
            print(f"  {cell:18s} ({mAh} mAh):  {hours/24:.1f} days  ({hours:.0f} h)")
        else:
            print(f"  {cell:18s} ({mAh} mAh):  {hours:.1f} h")
    print("=" * 64)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PPK2 trace analyzer (zephyr_tflite)")
    parser.add_argument("csv", nargs="?",
                        default="logs/ppk-20260608T000110.csv", type=Path)
    parser.add_argument("--vdd", type=float, default=3.0,
                        help="Supply voltage in volts (default 3.0)")
    parser.add_argument("--rolling-window", type=int, default=200,
                        help="Rolling-mean window in ms (default 200)")
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}")

    df = load_ppk2_csv(args.csv)
    mean_ua = float(df["i_ua"].mean())

    print_summary(df, mean_ua, args.vdd)

    plots_dir = args.csv.parent / "plots"
    plots_dir.mkdir(exist_ok=True)
    stem = args.csv.stem

    plot_full_trace(df, mean_ua, plots_dir / f"{stem}_trace.png")
    plot_rolling_stats(df, args.rolling_window, plots_dir / f"{stem}_rolling.png")
    plot_current_dist(df, mean_ua, plots_dir / f"{stem}_current_dist.png")
    plot_power_spectrum(df, plots_dir / f"{stem}_power_spectrum.png")


if __name__ == "__main__":
    main()
