'''
Plot ping RTTs over time
'''
from helper import *
import plot_defaults

from matplotlib.ticker import MaxNLocator
from pylab import figure

parser = argparse.ArgumentParser()
parser.add_argument('--files', '-f',
                    help="Ping output files to plot",
                    required=True,
                    action="store",
                    nargs='+')

parser.add_argument('--freq',
                    help="Frequency of pings (per second)",
                    type=int,
                    default=10)

parser.add_argument('--out', '-o',
                    help="Output png file for the plot.",
                    default=None)  # Will show the plot

args = parser.parse_args()


def parse_ping(fname):
    ret = []
    lines = open(fname).readlines()
    num = 0
    for line in lines:
        if 'bytes from' not in line:
            continue
        try:
            rtt = line.split(' ')[-2]
            rtt = rtt.split('=')[1]
            rtt = float(rtt)
            ret.append([num, rtt])
            num += 1
        except Exception:
            break
    return ret


m.rc('figure', figsize=(16, 6))
fig = figure()
ax = fig.add_subplot(111)
for i, f in enumerate(args.files):
    data = parse_ping(f)
    xs = [float(row[0]) for row in data]
    start_time = xs[0] if xs else 0
    xs = [(x - start_time) / args.freq for x in xs]
    rtts = [float(row[1]) for row in data]

    ax.plot(xs, rtts, lw=2)
    ax.xaxis.set_major_locator(MaxNLocator(4))

plt.ylabel("RTT (ms)")
plt.xlabel("Seconds")
plt.grid(True)

if args.out:
    print('saving to', args.out)
    plt.savefig(args.out)
else:
    plt.show()
