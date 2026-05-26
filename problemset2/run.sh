#!/bin/bash

# Note: Mininet must be run as root.  So invoke this shell script
# using sudo.

set -e

time=200
bwnet=1.5
# One-way propagation delay on the bottleneck link (ms).
# Only the bottleneck link (s0 - h2) is given a delay in BBTopo, so
# the minimum RTT between h1 and h2 is 2 * delay.  For RTT = 20ms we
# set delay = 10ms.
delay=10

iperf_port=5001

# Clean up any stragglers from a previous run, then make sure mininet
# is in a clean state before we start.
mn -c >/dev/null 2>&1 || true

for qsize in 100 20; do
    dir=bb-q$qsize

    python3 bufferbloat.py -b $bwnet --delay $delay --maxq $qsize \
        --dir $dir -t $time --cong reno

    python3 plot_tcpprobe.py -f $dir/cwnd.txt -o cwnd-q$qsize.png   -p $iperf_port
    python3 plot_queue.py -f $dir/q.txt    -o buffer-q$qsize.png
    python3 plot_ping.py -f $dir/ping.txt -o rtt-q$qsize.png
done
