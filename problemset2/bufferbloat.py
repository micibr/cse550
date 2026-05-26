#!/usr/bin/env python3
"Problem Set 2: Bufferbloat"

from mininet.topo import Topo
from mininet.node import CPULimitedHost, OVSBridge
from mininet.link import TCLink
from mininet.net import Mininet
from mininet.log import lg, info
from mininet.util import dumpNodeConnections
from mininet.cli import CLI

from subprocess import Popen, PIPE
from time import sleep, time
from multiprocessing import Process
from argparse import ArgumentParser

from monitor import monitor_qlen

import sys
import os
import math
import statistics

parser = ArgumentParser(description="Bufferbloat tests")
parser.add_argument('--bw-host', '-B',
                    type=float,
                    help="Bandwidth of host links (Mb/s)",
                    default=1000)

parser.add_argument('--bw-net', '-b',
                    type=float,
                    help="Bandwidth of bottleneck (network) link (Mb/s)",
                    required=True)

parser.add_argument('--delay',
                    type=float,
                    help="Link propagation delay (ms)",
                    required=True)

parser.add_argument('--dir', '-d',
                    help="Directory to store outputs",
                    required=True)

parser.add_argument('--time', '-t',
                    help="Duration (sec) to run the experiment",
                    type=int,
                    default=10)

parser.add_argument('--maxq',
                    type=int,
                    help="Max buffer size of network interface in packets",
                    default=100)

# Linux uses CUBIC-TCP by default that doesn't have the usual sawtooth
# behaviour.  For those who are curious, invoke this script with
# --cong cubic and see what happens...
# sysctl -a | grep cong should list some interesting parameters.
parser.add_argument('--cong',
                    help="Congestion control algorithm to use",
                    default="reno")

# Expt parameters
args = parser.parse_args()


class BBTopo(Topo):
    "Simple topology for bufferbloat experiment."

    def build(self, n=2):
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')

        # Here I have created a switch.  If you change its name, its
        # interface names will change from s0-eth1 to newname-eth1.
        switch = self.addSwitch('s0')

        # h1 -- switch: fast 1Gb/s "home LAN" link, no added delay/queue.
        self.addLink(h1, switch, bw=args.bw_host)

        # switch -- h2: slow 1.5 Mb/s bottleneck uplink with the
        # configured one-way propagation delay and limited queue size.
        # One-way delay = args.delay ms  =>  minimum RTT = 2 * delay.
        self.addLink(switch, h2,
                     bw=args.bw_net,
                     delay='%fms' % args.delay,
                     max_queue_size=args.maxq)


# tcp_probe is gone from modern kernels, so we sample sender-side cwnd
# by polling `ss -ti` for the long-lived iperf flow inside h1's network
# namespace.  Output format per line: "<unix_time>,<cwnd_in_mss>".
def start_cwnd_monitor(net, iperf_port=5001, interval=0.1,
                       outfile="cwnd.txt"):
    h1 = net.get('h1')
    script = (
        "while true; do "
        "T=$(date +%%s.%%N); "
        "C=$(ss -tin state established '( dport = :%d or sport = :%d )' "
        "2>/dev/null | grep -oE 'cwnd:[0-9]+' | head -1 | cut -d: -f2); "
        "if [ -n \"$C\" ]; then echo \"$T,$C\" >> %s; fi; "
        "sleep %f; "
        "done"
    ) % (iperf_port, iperf_port, outfile, interval)
    # Truncate output file first so re-runs start clean.
    open(outfile, 'w').close()
    return h1.popen(script, shell=True)


def start_qmon(iface, interval_sec=0.1, outfile="q.txt"):
    monitor = Process(target=monitor_qlen,
                      args=(iface, interval_sec, outfile))
    monitor.start()
    return monitor


def start_iperf(net):
    h1 = net.get('h1')
    h2 = net.get('h2')
    print("Starting iperf server on h2...")
    # The -w 16m parameter ensures the TCP flow is not receiver window
    # limited.  Otherwise the router buffer may not fill up.
    server = h2.popen("iperf -s -w 16m", shell=True)
    print("Starting iperf client on h1...")
    # Long-lived TCP flow from h1 -> h2 for the full experiment.
    client = h1.popen("iperf -c %s -t %d -w 16m"
                      % (h2.IP(), args.time + 5), shell=True)
    return [server, client]


def start_webserver(net):
    h1 = net.get('h1')
    proc = h1.popen("python3 http/webserver.py", shell=True)
    sleep(1)
    return [proc]


def start_ping(net):
    h1 = net.get('h1')
    h2 = net.get('h2')
    # 10 pings/sec from h1 to h2.  -D timestamps each reply.  We let it
    # run for the whole experiment; bufferbloat() will terminate it.
    cmd = "ping -i 0.1 -w %d %s > %s/ping.txt" % (
        args.time + 5, h2.IP(), args.dir)
    return h1.popen(cmd, shell=True)


def fetch_webpage(net):
    """Fetch index.html from h1's webserver (run from h2) and return
    the total fetch time in seconds, or None on failure.  Data flows
    h1 -> h2, in the same direction as the iperf long-lived flow."""
    h1 = net.get('h1')
    h2 = net.get('h2')
    cmd = ("curl -o /dev/null -s -w '%%{time_total}' "
           "--connect-timeout 5 --max-time 30 "
           "http://%s/index.html") % h1.IP()
    out = h2.popen(cmd, shell=True, stdout=PIPE).communicate()[0]
    try:
        return float(out.decode().strip())
    except (ValueError, AttributeError):
        return None


def bufferbloat():
    if not os.path.exists(args.dir):
        os.makedirs(args.dir)
    os.system("sysctl -w net.ipv4.tcp_congestion_control=%s" % args.cong)
    topo = BBTopo()
    net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink,
                  switch=OVSBridge, controller=None)
    net.start()
    # This dumps the topology and how nodes are interconnected through
    # links.
    dumpNodeConnections(net.hosts)
    # This performs a basic all pairs ping test.
    net.pingAll()

    # Start cwnd sampling (replacement for tcp_probe).
    cwnd_mon = start_cwnd_monitor(net, iperf_port=5001,
                                  outfile='%s/cwnd.txt' % args.dir)

    # Monitor the queue on the bottleneck egress (s0 -> h2).  Because
    # h1 was added first (s0-eth1) and h2 second, the link toward h2 is
    # s0-eth2.
    qmon = start_qmon(iface='s0-eth2',
                      outfile='%s/q.txt' % (args.dir))

    iperf_procs = start_iperf(net)
    ping_proc = start_ping(net)
    web_procs = start_webserver(net)

    # Periodically fetch index.html from h1's webserver: 3 back-to-back
    # fetches every 5 seconds.  The webpage data flows h1 -> h2, same
    # direction as the long-lived iperf flow.
    fetch_times = []
    start_time = time()
    while True:
        for _ in range(3):
            ft = fetch_webpage(net)
            if ft is not None:
                fetch_times.append(ft)
        sleep(5)
        now = time()
        delta = now - start_time
        if delta > args.time:
            break
        print("%.1fs left..." % (args.time - delta))

    # Summary statistics for the webpage fetch times.
    if fetch_times:
        mean = statistics.mean(fetch_times)
        sd = statistics.stdev(fetch_times) if len(fetch_times) > 1 else 0.0
        print("Webpage fetch: n=%d avg=%.4fs stdev=%.4fs"
              % (len(fetch_times), mean, sd))
        with open('%s/fetch_stats.txt' % args.dir, 'w') as f:
            f.write("n=%d\navg=%.6f\nstdev=%.6f\n"
                    % (len(fetch_times), mean, sd))
            f.write("samples=" + ",".join("%.6f" % t for t in fetch_times)
                    + "\n")
    else:
        print("Webpage fetch: no successful fetches!")

    # Hint: The command below invokes a CLI which you can use to
    # debug.  It allows you to run arbitrary commands inside your
    # emulated hosts h1 and h2.
    # CLI(net)

    # Tear down monitors and helper processes.
    try:
        cwnd_mon.terminate()
    except Exception:
        pass
    qmon.terminate()
    try:
        ping_proc.terminate()
    except Exception:
        pass
    for p in iperf_procs + web_procs:
        try:
            p.terminate()
        except Exception:
            pass

    net.stop()
    # Ensure that all processes you create within Mininet are killed.
    # Sometimes they require manual killing.
    Popen("pgrep -f webserver.py | xargs kill -9", shell=True).wait()
    Popen("pgrep -f iperf | xargs kill -9", shell=True).wait()


if __name__ == "__main__":
    bufferbloat()
