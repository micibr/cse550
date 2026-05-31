import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

# Timestamp like "[00:00:02.121,368]" -> seconds (float).
# Format is HH:MM:SS.mmm,uuu (milliseconds, microseconds).
TS_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.(\d{3}),(\d{3})\]")

# ML result line, e.g.:
#   e:ml_result_event sine val: 1.00 anomaly: -0.07
ML_RE = re.compile(
    r"e:ml_result_event\s+(\S+)\s+val:\s*(-?\d+\.\d+)\s+anomaly:\s*(-?\d+\.\d+)"
)


def ts_to_seconds(match: re.Match) -> float:
    h, m, s, ms, us = map(int, match.groups())
    return h * 3600 + m * 60 + s + ms / 1e3 + us / 1e6


def parse_log(path: Path):
    """Return (ml_events, system_events).

    ml_events: list of dicts with keys t, label, val, anomaly
    system_events: list of dicts with keys t, kind, detail
    """
    ml_events = []
    system_events = []

    # Things worth marking on the timeline. (substring_to_find, label, kind)
    markers = [
        ("Use fast advertising", "BLE: fast adv", "ble"),
        ("Use slow advertising", "BLE: slow adv", "ble"),
        ("Advertising started", "BLE: adv started", "ble"),
        ("Advertising stopped", "BLE: adv stopped", "ble"),
        ("ml_app_mode_event state: MODEL_RUNNING", "ML: model running", "ml"),
        ("System power down", "System: power down", "power"),
        ("Power down the board", "System: powered off", "power"),
        ("Bluetooth initialized", "BLE: stack init", "ble"),
    ]

    with path.open() as f:
        for line in f:
            ts_match = TS_RE.search(line)
            if not ts_match:
                continue
            t = ts_to_seconds(ts_match)

            ml_match = ML_RE.search(line)
            if ml_match:
                ml_events.append(
                    {
                        "t": t,
                        "label": ml_match.group(1),
                        "val": float(ml_match.group(2)),
                        "anomaly": float(ml_match.group(3)),
                    }
                )
                continue

            for needle, label, kind in markers:
                if needle in line:
                    system_events.append({"t": t, "label": label, "kind": kind})
                    break

    return ml_events, system_events


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

kind_colors = {"ble": "#2ca02c", "power": "#9467bd", "ml": "#ff7f0e"}


def plot_timeseries(ml_events, system_events, out_path: Path):
    t = np.array([e["t"] for e in ml_events])
    val = np.array([e["val"] for e in ml_events])
    anomaly = np.array([e["anomaly"] for e in ml_events])

    fig, ax1 = plt.subplots(figsize=(12, 4))
    ax1.plot(t, val, "o-", color="#1f77b4", markersize=4,
             linewidth=1.2, label="Classification confidence (val)")
    ax1.set_ylabel("Confidence", color="#1f77b4")
    ax1.set_ylim(0.95, 1.01)
    ax1.tick_params(axis="y", labelcolor="#1f77b4")

    ax1b = ax1.twinx()
    ax1b.plot(t, anomaly, "s-", color="#d62728", markersize=3,
              linewidth=1.0, alpha=0.75, label="Anomaly score")
    ax1b.set_ylabel("Anomaly score", color="#d62728")
    ax1b.tick_params(axis="y", labelcolor="#d62728")

    seen_kinds = set()
    for ev in system_events:
        color = kind_colors.get(ev["kind"], "gray")
        label = ev["kind"].upper() if ev["kind"] not in seen_kinds else None
        seen_kinds.add(ev["kind"])
        ax1.axvline(ev["t"], color=color, linestyle="--",
                    alpha=0.4, linewidth=1, label=label)

    ax1.set_xlabel("Time since boot (s)")
    ax1.set_title("ML inference over time (confidence + anomaly score)",
                  fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1b.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc="lower left", fontsize=8, ncol=2)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure to {out_path}")
    plt.close(fig)


def plot_anomaly_dist(ml_events, out_path: Path):
    anomaly = np.array([e["anomaly"] for e in ml_events])

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(anomaly, bins=15, color="#d62728", alpha=0.75, edgecolor="black")
    ax.axvline(anomaly.mean(), color="black", linestyle="--",
               linewidth=1.5, label=f"mean = {anomaly.mean():.3f}")
    ax.axvline(0, color="gray", linestyle=":", linewidth=1,
               label="anomaly threshold (0)")
    ax.set_xlabel("Anomaly score")
    ax.set_ylabel("Count")
    ax.set_title("Anomaly score distribution\n(all scores < 0 → model is confident in-distribution)",
                 fontsize=10, fontweight="bold")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure to {out_path}")
    plt.close(fig)


def plot_inference_cadence(ml_events, out_path: Path):
    t = np.array([e["t"] for e in ml_events])
    intervals = np.diff(t) * 1000.0  # ms between consecutive inferences

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(t[1:], intervals, "o-", color="#ff7f0e", markersize=3, linewidth=1)
    ax.axhline(intervals.mean(), color="black", linestyle="--",
               linewidth=1.2, label=f"mean = {intervals.mean():.1f} ms")
    ax.fill_between(
        t[1:],
        intervals.mean() - intervals.std(),
        intervals.mean() + intervals.std(),
        color="black", alpha=0.08, label=f"±1σ ({intervals.std():.2f} ms)",
    )
    ax.set_xlabel("Time since boot (s)")
    ax.set_ylabel("Interval between inferences (ms)")
    ax.set_title("Inference cadence / jitter", fontsize=10, fontweight="bold")
    ax.ticklabel_format(axis="y", useOffset=False, style="plain")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved figure to {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Stats summary (printed to stdout)
# ---------------------------------------------------------------------------

def print_summary(ml_events, system_events):
    t = np.array([e["t"] for e in ml_events])
    val = np.array([e["val"] for e in ml_events])
    anomaly = np.array([e["anomaly"] for e in ml_events])
    intervals = np.diff(t) * 1000.0

    labels = {e["label"] for e in ml_events}

    print("=" * 60)
    print("LOG SUMMARY")
    print("=" * 60)
    print(f"ML inference events:       {len(ml_events)}")
    print(f"Labels predicted:          {', '.join(sorted(labels))}")
    print(f"Runtime (first→last ML):   {t[-1] - t[0]:.2f} s")
    print()
    print("Confidence (val):")
    print(f"  min / mean / max:        {val.min():.3f} / {val.mean():.3f} / {val.max():.3f}")
    print()
    print("Anomaly score:")
    print(f"  min / mean / max:        {anomaly.min():.3f} / {anomaly.mean():.3f} / {anomaly.max():.3f}")
    print(f"  all below 0 (in-dist):   {(anomaly < 0).all()}")
    print()
    print("Inference interval (ms):")
    print(f"  mean:                    {intervals.mean():.2f}")
    print(f"  std (jitter):            {intervals.std():.2f}")
    print(f"  min / max:               {intervals.min():.2f} / {intervals.max():.2f}")
    print()
    print(f"System events captured:    {len(system_events)}")
    for ev in system_events:
        print(f"  t={ev['t']:7.3f}s  {ev['label']}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("device.log")
    if not log_path.exists():
        raise SystemExit(f"Log file not found: {log_path}")

    ml_events, system_events = parse_log(log_path)
    if not ml_events:
        raise SystemExit("No ml_result_event lines found in log — nothing to plot.")

    print_summary(ml_events, system_events)

    plots_dir = log_path.parent / "plots"
    plots_dir.mkdir(exist_ok=True)
    stem = log_path.stem
    plot_timeseries(ml_events, system_events, plots_dir / f"{stem}_timeseries.png")
    plot_anomaly_dist(ml_events, plots_dir / f"{stem}_anomaly_dist.png")
    plot_inference_cadence(ml_events, plots_dir / f"{stem}_inference_cadence.png")


if __name__ == "__main__":
    main()
