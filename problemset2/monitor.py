from time import sleep, time
from subprocess import Popen, PIPE
import re

default_dir = '.'

def monitor_qlen(iface, interval_sec=0.1, fname='%s/qlen.txt' % default_dir):
    pat_queued = re.compile(r'backlog\s[^\s]+\s([\d]+)p')
    cmd = "tc -s qdisc show dev %s" % (iface)
    open(fname, 'w').write('')
    while True:
        p = Popen(cmd, shell=True, stdout=PIPE)
        output = p.stdout.read().decode('utf-8', errors='ignore')
        matches = pat_queued.findall(output)
        if matches and len(matches) > 1:
            t = "%f" % time()
            with open(fname, 'a') as f:
                f.write(t + ',' + matches[1] + '\n')
        sleep(interval_sec)

def monitor_devs_ng(fname="%s/txrate.txt" % default_dir, interval_sec=0.01):
    """Uses bwm-ng tool to collect iface tx rate stats.  Very reliable."""
    cmd = ("sleep 1; bwm-ng -t %s -o csv "
           "-u bits -T rate -C ',' > %s" %
           (interval_sec * 1000, fname))
    Popen(cmd, shell=True).wait()
