'''
Helper module for the plot scripts.
'''

import re
import itertools
import matplotlib as m
import os
m.use("Agg")
import matplotlib.pyplot as plt
import argparse
import math

def read_list(fname, delim=','):
    lines = open(fname).readlines()
    ret = []
    for l in lines:
        ls = l.strip().split(delim)
        ls = [('0' if e.strip() in ('', 'ms', 's') else e) for e in ls]
        ret.append(ls)
    return ret

def ewma(alpha, values):
    if alpha == 0:
        return values
    ret = []
    prev = 0
    for v in values:
        prev = alpha * prev + (1 - alpha) * v
        ret.append(prev)
    return ret

def col(n, obj=None, clean=lambda e: e):
    """A versatile column extractor."""
    if obj is None:
        def f(item):
            return clean(item[n])
        return f
    if isinstance(obj, list):
        if len(obj) > 0 and isinstance(obj[0], (list, dict)):
            return [clean(item[n]) for item in obj]
    if isinstance(obj, (list, dict)):
        try:
            return clean(obj[n])
        except Exception:
            return None
    return None

def transpose(l):
    return list(zip(*l))

def avg(lst):
    lst = list(lst)
    return sum(map(float, lst)) / len(lst)

def stdev(lst):
    lst = list(lst)
    mean = avg(lst)
    var = avg([(float(e) - mean) ** 2 for e in lst])
    return math.sqrt(var)

def xaxis(values, limit):
    values = list(values)
    l = len(values)
    return list(zip(*[(x * 1.0 * limit / l, y) for (x, y) in enumerate(values)]))

def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(fillvalue=fillvalue, *args)

def cdf(values):
    values = sorted(values)
    prob = 0
    l = len(values)
    x, y = [], []
    for v in values:
        prob += 1.0 / l
        x.append(v)
        y.append(prob)
    return (x, y)

def pc95(lst):
    l = len(lst)
    return sorted(lst)[int(0.95 * l)]

def pc99(lst):
    l = len(lst)
    return sorted(lst)[int(0.99 * l)]

def coeff_variation(lst):
    return stdev(lst) / avg(lst)
