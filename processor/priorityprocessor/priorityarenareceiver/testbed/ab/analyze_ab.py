#!/usr/bin/env python3
# Tomislav-RetCtx: A/B harness for the priority pipelines; see ../../README.md and RESULTS.md in the run directory.
"""Summarize A/B runs: per variant and per load step, what the load offered and got acked,
what reached the sink, the collector's CPU, memory and GC. Usage: analyze_ab.py RUN_DIR..."""
import json
import pathlib
import re
import statistics
import sys

GC = re.compile(r'^gc (\d+) @([\d.]+)s (\d+)%: .*?(\d+)->(\d+)->(\d+) MB, (\d+) MB goal')


def load_lines(p):
    return [json.loads(l) for l in p.read_text().splitlines() if l.startswith('{')]


def cgroup(p):
    rows = [l.split() for l in p.read_text().splitlines()[1:] if len(l.split()) == 8]
    out = []
    for a, b in zip(rows, rows[1:]):
        dt = float(b[0]) - float(a[0])
        out.append({'t': float(b[0]), 'cpu': (int(b[1]) - int(a[1])) / 1e6 / dt,
                    'throttled': (int(b[3]) - int(a[3])) / 1e6 / dt, 'mem': int(b[4]) / 2**20,
                    'anon': int(b[5]) / 2**20, 'oom': int(b[7])})
    return out


def gctrace(p):
    return [(float(m[2]), int(m[3]), int(m[5]), int(m[6]), int(m[7]))
            for l in p.read_text().splitlines() if (m := GC.match(l))]


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else 0


def steps(meta):
    t, out = 0.0, []
    for part in meta['load_steps'].split(','):
        d, r = part.split(':')
        d = float(d.rstrip('s'))
        out.append((t, t + d, float(r)))
        t += d
    return out


def summarize(vdir):
    meta = json.load(open(vdir / 'meta.json'))
    load = [l for l in load_lines(vdir / 'load.jsonl') if not l.get('final')]
    final = load_lines(vdir / 'load.jsonl')[-1]
    sink = load_lines(vdir / 'sink.jsonl')
    cg = cgroup(vdir / 'cgroup.tsv')
    gcs = gctrace(vdir / 'agent.log')
    t0 = cg[0]['t'] - 1 if cg else 0
    # the load and the cgroup clock differ by the agent's start; align on the load start
    load_start = meta['started']
    rows = []
    for a, b, rate in steps(meta):
        # cumulative counters at the window edges (skip the first 10 s of each step)
        def at(t):
            best = None
            for l in load:
                if l['t'] <= t:
                    best = l
            return best
        lo, hi = at(a + 10), at(b)
        if not lo or not hi:
            continue
        span = hi['t'] - lo['t']
        r = {'rate': rate, 'secs': round(span)}
        for c in ('hp', 'lp'):
            off = hi[c]['offered'] - lo[c]['offered']
            ack = hi[c]['acked'] - lo[c]['acked']
            ref = sum(hi[c]['refused'].values()) - sum(lo[c]['refused'].values())
            drop = hi[c]['dropped'] - lo[c]['dropped']
            r[c + '_ack%'] = 100 * ack / off if off else 0
            r[c + '_refused%'] = 100 * ref / off if off else 0
            r[c + '_dropped%'] = 100 * drop / off if off else 0
            p99 = [l[c]['p99_ms'] for l in load if a + 10 < l['t'] <= b and l[c]['p99_ms']]
            r[c + '_p99ms'] = round(pct(p99, 0.5), 1)
            r[c + '_maxms'] = round(max([l[c]['max_ms'] for l in load if a + 10 < l['t'] <= b] or [0]), 1)
        win = [x for x in cg if load_start + a + 10 < x['t'] <= load_start + b]
        r['cpu'] = round(statistics.mean(x['cpu'] for x in win), 3) if win else None
        r['throttled'] = round(statistics.mean(x['throttled'] for x in win), 3) if win else None
        r['mem_max'] = round(max(x['mem'] for x in win), 1) if win else None
        agent_t0 = cg[0]['t'] - 1 if cg else load_start
        gw = [g for g in gcs if load_start + a + 10 < agent_t0 + g[0] <= load_start + b]
        r['gc/s'] = round(len(gw) / span, 2) if span else 0
        r['heap_peak_mb'] = max((g[3] for g in gw), default=0)
        rows.append(r)
    sink_last = sink[-1] if sink else {}
    totals = {}
    for c in ('hp', 'lp'):
        acked = final[c]['acked']
        totals[c] = {'offered': final[c]['offered'], 'acked': acked, 'refused': sum(final[c]['refused'].values()),
                     'dropped': final[c]['dropped'], 'delivered': sink_last.get(c + '_spans', 0),
                     'lost_after_ack': acked - sink_last.get(c + '_spans', 0),
                     'refused_codes': final[c]['refused']}
    gc_total = gcs[-1][1] if gcs else None
    oom = max((x['oom'] for x in cg), default=0)
    return meta, rows, totals, {'gc_cycles': len(gcs), 'gc_cpu_pct_final': gc_total, 'oom_kills': oom,
                                'mem_max_mb': round(max((x['mem'] for x in cg), default=0), 1),
                                'sink_requests': sink_last.get('hp_requests', 0) + sink_last.get('lp_requests', 0)}


def timeline(vdir, window=15):
    """Per window: offered/acked/refused/dropped at the load, delivered at the sink, per class;
    the collector's CPU, memory and GCs."""
    meta = json.load(open(vdir / 'meta.json'))
    load = [l for l in load_lines(vdir / 'load.jsonl') if not l.get('final')]
    sink = load_lines(vdir / 'sink.jsonl')
    cg = cgroup(vdir / 'cgroup.tsv')
    gcs = gctrace(vdir / 'agent.log')
    t0 = meta['started']
    agent_t0 = cg[0]['t'] - 1 if cg else t0
    # the sink starts ~1 s before the load; its clock is close enough at this resolution
    end = max(l['t'] for l in load)
    print(f'{"t":>5} | {"hp ack":>7} {"ref":>6} {"drop":>6} {"dlvd":>7} | {"lp ack":>7} {"ref":>7} {"drop":>6} {"dlvd":>7} | {"cap":>6} | {"cpu":>5} {"thr":>5} {"memMB":>6} {"gc/s":>5}')

    def cum(rows, t, f):
        best = None
        for r in rows:
            if r['t'] <= t:
                best = r
        return f(best) if best else 0
    for a in range(0, int(end) + 1, window):
        b = a + window
        def d(rows, f):
            return cum(rows, b, f) - cum(rows, a, f)
        row = []
        for c in ('hp', 'lp'):
            row += [d(load, lambda r: r[c]['acked']), d(load, lambda r: sum(r[c]['refused'].values())),
                    d(load, lambda r: r[c]['dropped']), d(sink, lambda r: r[c + '_spans'])]
        capr = [s['capacity'] for s in sink if a <= s['t'] < b]
        cap = 'inf' if capr and min(capr) < 0 else (int(min(capr)) if capr else '?')
        win = [x for x in cg if t0 + a < x['t'] <= t0 + b]
        cpu = statistics.mean(x['cpu'] for x in win) if win else 0
        thr = statistics.mean(x['throttled'] for x in win) if win else 0
        mem = max((x['mem'] for x in win), default=0)
        ngc = len([g for g in gcs if t0 + a < agent_t0 + g[0] <= t0 + b])
        r = [x // 1000 for x in row]  # thousands of spans
        print(f'{a:5d} | {r[0]:6d}k {r[1]:5d}k {r[2]:5d}k {r[3]:6d}k | {r[4]:6d}k {r[5]:6d}k {r[6]:5d}k {r[7]:6d}k | {str(cap):>6} | {cpu:5.2f} {thr:5.2f} {mem:6.1f} {ngc / window:5.1f}')


def main():
    if sys.argv[1] == '--timeline':
        for vdir in map(pathlib.Path, sys.argv[2:]):
            print(f'\n### {vdir.parent.name} / {vdir.name}')
            timeline(vdir)
        return
    for run in map(pathlib.Path, sys.argv[1:]):
        for vdir in sorted(p for p in run.iterdir() if (p / 'meta.json').exists()):
            meta, rows, totals, other = summarize(vdir)
            print(f'\n### {run.name} / {vdir.name}  ({meta["scenario"]}; sink {meta["sink_steps"]})')
            cols = list(rows[0].keys()) if rows else []
            print(' '.join(f'{c:>12}' for c in cols))
            for r in rows:
                print(' '.join(f'{(round(v, 1) if isinstance(v, float) else v):>12}' for v in r.values()))
            for c, t in totals.items():
                print(f'  {c}: ' + ', '.join(f'{k} {v}' for k, v in t.items()))
            print('  ' + ', '.join(f'{k} {v}' for k, v in other.items()))


if __name__ == '__main__':
    main()
