'''
Plot cwnd time-series.

The legacy tcp_probe kernel module is unavailable on modern kernels, so
bufferbloat.py samples cwnd by polling `ss -ti` for the long-lived TCP
flow and writes simple CSV output:

    <unix_timestamp_seconds>,<cwnd_in_mss_segments>

This plotter reads that CSV.  The `-p`/`--port` argument is accepted for
backwards compatibility with run.sh but is unused (the monitor in
bufferbloat.py already filters by port at capture time).
'''
from helper import *
import plot_defaults

from matplotlib.ticker import MaxNLocator
from pylab import figure
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', dest="files", nargs='+', required=True)
parser.add_argument('-o', '--out', dest="out", default=None)
parser.add_argument('-p', '--port', dest="port", default='5001',
                    help="(Unused; kept for compatibility with run.sh.)")
parser.add_argument('--sport', action='store_true', default=False,
                    help="(Unused; kept for compatibility with run.sh.)")
parser.add_argument('-H', '--histogram', dest="histogram",
                    action="store_true", default=False)

args = parser.parse_args()

MSS_BYTES = 1448  # typical Linux MSS over Ethernet


def parse_cwnd(fname):
    times, cwnd_kb = [], []
    for line in open(fname):
        line = line.strip()
        if not line:
            continue
        parts = line.split(',')
        if len(parts) < 2:
            continue
        try:
            t = float(parts[0])
            c = int(parts[1])
        except ValueError:
            continue
        times.append(t)
        cwnd_kb.append(c * MSS_BYTES / 1024.0)
    return times, cwnd_kb


m.rc('figure', figsize=(16, 6))
fig = plt.figure()
plots = 2 if args.histogram else 1
ax = fig.add_subplot(1, plots, 1)

all_cwnds = []
for f in args.files:
    times, cwnd_kb = parse_cwnd(f)
    if not times:
        print("warning: no cwnd samples in %s" % f)
        continue
    t0 = times[0]
    times = [t - t0 for t in times]
    ax.plot(times, cwnd_kb, lw=2)
    all_cwnds.extend(cwnd_kb)

ax.grid(True)
ax.set_xlabel("seconds")
ax.set_ylabel("cwnd (KB)")
ax.set_title("TCP congestion window (cwnd) timeseries")
ax.xaxis.set_major_locator(MaxNLocator(6))

if args.histogram and all_cwnds:
    axHist = fig.add_subplot(1, 2, 2)
    axHist.hist(all_cwnds, 50, density=True, facecolor='green', alpha=0.75)
    axHist.set_xlabel("bins (KB)")
    axHist.set_ylabel("Fraction")
    axHist.set_title("Histogram of cwnd")

if args.out:
    print('saving to', args.out)
    plt.savefig(args.out)
else:
    plt.show()
