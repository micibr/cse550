import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

ROW_RE = re.compile(
    r"x:\s*(-?\d+\.\d+)\s+pred:\s*(-?\d+\.\d+)\s+true:\s*(-?\d+\.\d+)\s+err:\s*(-?\d+\.\d+)"
)


def parse_log(path: Path):
    """Return list of dicts with keys x, pred, true, err."""
    rows = []
    with path.open() as f:
        for line in f:
            m = ROW_RE.search(line)
            if m:
                rows.append(
                    {
                        "x": float(m.group(1)),
                        "pred": float(m.group(2)),
                        "true": float(m.group(3)),
                        "err": float(m.group(4)),
                    }
                )
    return rows


def split_cycles(rows):
    """Split rows into cycles.  A new cycle begins at x ≈ 0 (the reset marker)."""
    cycles = []
    current = []
    for row in rows:
        if row["x"] == 0.0 and current:
            cycles.append(current)
            current = []
        else:
            current.append(row)
    if current:
        cycles.append(current)
    return cycles


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def plot_pred_vs_true(rows, out_path: Path):
    """Overlay predicted and true sine values over x."""
    xs = np.array([r["x"] for r in rows])
    preds = np.array([r["pred"] for r in rows])
    trues = np.array([r["true"] for r in rows])

    order = np.argsort(xs)
    xs_s, preds_s, trues_s = xs[order], preds[order], trues[order]

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.scatter(xs, preds, s=6, alpha=0.25, color="#1f77b4", label="Predicted (all cycles)")
    ax.plot(xs_s, trues_s, color="#d62728", linewidth=2, label="True sin(x)", zorder=5)
    ax.set_xlabel("x (radians)")
    ax.set_ylabel("sin(x)")
    ax.set_title("TFLite inference: predicted vs. true sin(x)", fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


def plot_error_vs_x(rows, out_path: Path):
    """Absolute error vs. x with per-x mean ± 1σ envelope."""
    xs = np.array([r["x"] for r in rows])
    errs = np.abs(np.array([r["err"] for r in rows]))

    unique_x = np.unique(xs)
    means, stds = [], []
    for ux in unique_x:
        mask = xs == ux
        means.append(errs[mask].mean())
        stds.append(errs[mask].std())
    means, stds = np.array(means), np.array(stds)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.scatter(xs, errs, s=5, alpha=0.20, color="#ff7f0e", label="|error| (all cycles)")
    ax.plot(unique_x, means, color="black", linewidth=1.5, label="Mean |error| per x")
    ax.fill_between(
        unique_x, means - stds, means + stds,
        color="black", alpha=0.10, label="±1σ",
    )
    ax.set_xlabel("x (radians)")
    ax.set_ylabel("|error|")
    ax.set_title("Prediction absolute error vs. input x", fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


def plot_error_dist(rows, out_path: Path):
    """Histogram of signed prediction errors."""
    errs = np.array([r["err"] for r in rows])

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(errs, bins=30, color="#2ca02c", alpha=0.75, edgecolor="black")
    ax.axvline(errs.mean(), color="black", linestyle="--", linewidth=1.5,
               label=f"mean = {errs.mean():.4f}")
    ax.axvline(0, color="gray", linestyle=":", linewidth=1, label="zero error")
    ax.set_xlabel("Prediction error  (pred − true)")
    ax.set_ylabel("Count")
    ax.set_title("Error distribution across all inferences", fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


def plot_cycle_consistency(cycles, out_path: Path):
    """Box-plot of |error| per inference step, aggregated across cycles."""
    if len(cycles) < 2:
        print("Not enough cycles for consistency plot — skipping.")
        return

    step_len = min(len(c) for c in cycles)
    step_errors = [
        [abs(cycles[ci][si]["err"]) for ci in range(len(cycles)) if si < len(cycles[ci])]
        for si in range(step_len)
    ]
    step_xs = [cycles[0][si]["x"] for si in range(step_len)]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.boxplot(step_errors, positions=range(step_len), widths=0.5,
               patch_artist=True,
               boxprops=dict(facecolor="#aec7e8", color="#1f77b4"),
               medianprops=dict(color="#d62728", linewidth=1.5),
               flierprops=dict(marker=".", markersize=3, alpha=0.5))
    ax.set_xticks(range(step_len))
    ax.set_xticklabels([f"{v:.2f}" for v in step_xs], rotation=45, fontsize=7)
    ax.set_xlabel("x (radians)")
    ax.set_ylabel("|error|")
    ax.set_title(
        f"Cycle-to-cycle consistency: |error| per x across {len(cycles)} cycles",
        fontweight="bold",
    )
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"Saved {out_path}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Stats summary
# ---------------------------------------------------------------------------


def print_summary(rows, cycles):
    errs = np.array([r["err"] for r in rows])
    abs_errs = np.abs(errs)
    preds = np.array([r["pred"] for r in rows])
    trues = np.array([r["true"] for r in rows])

    rmse = np.sqrt(np.mean(errs ** 2))

    print("=" * 60)
    print("LOG SUMMARY  (zephyr_tflite / sine regression)")
    print("=" * 60)
    print(f"Total inference samples:   {len(rows)}")
    print(f"Inference cycles:          {len(cycles)}")
    print(f"Samples per cycle:         {len(cycles[0]) if cycles else 'N/A'}")
    print()
    print("Prediction error (pred − true):")
    print(f"  mean:                    {errs.mean():.5f}")
    print(f"  std:                     {errs.std():.5f}")
    print(f"  min / max:               {errs.min():.5f} / {errs.max():.5f}")
    print(f"  MAE:                     {abs_errs.mean():.5f}")
    print(f"  RMSE:                    {rmse:.5f}")
    print()
    print("Worst-error sample:")
    idx = int(np.argmax(abs_errs))
    r = rows[idx]
    print(f"  x={r['x']:.6f}  pred={r['pred']:.6f}  true={r['true']:.6f}  err={r['err']:.6f}")
    print()
    print(f"Predictions in [-1, 1]:    {((preds >= -1) & (preds <= 1)).mean() * 100:.1f}%")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("logs/serial-terminal-07062026_161522.txt")
    if not log_path.exists():
        raise SystemExit(f"Log file not found: {log_path}")

    rows = parse_log(log_path)
    if not rows:
        raise SystemExit("No inference rows found in log — nothing to plot.")

    cycles = split_cycles(rows)
    print_summary(rows, cycles)

    plots_dir = log_path.parent / "plots"
    plots_dir.mkdir(exist_ok=True)
    stem = log_path.stem

    plot_pred_vs_true(rows, plots_dir / f"{stem}_pred_vs_true.png")
    plot_error_vs_x(rows, plots_dir / f"{stem}_error_vs_x.png")
    plot_error_dist(rows, plots_dir / f"{stem}_error_dist.png")
    plot_cycle_consistency(cycles, plots_dir / f"{stem}_cycle_consistency.png")


if __name__ == "__main__":
    main()
