#!/usr/bin/env python3
# Tomislav-RetCtx: A/B harness for the priority pipelines; see ../../README.md and RESULTS.md in the run directory.
"""One A/B scenario for one collector variant.

sink (fake gateway) on node-2, collector under test on node-1 (0.5 CPU, 256 MiB, GOMAXPROCS 1,
as the deployed agents), load on node-3. The collector's cgroup is sampled every second.
Results: <out>/<variant>/{load.jsonl, sink.jsonl, agent.log, cgroup.tsv, meta.json}.
"""
import argparse
import json
import pathlib
import subprocess
import sys
import time

AGENT, SINK, LOAD = 'node-1', 'node-2', 'node-3'
AGENT_IP = '10.10.1.2'
IMAGE = 'registry.k8s.io/pause:3.9'
ENV = {
    'old': {'GOMEMLIMIT': '230MiB'},   # the deployed agents' setting
    'new': {'GOMEMLIMIT': '96MiB'},    # 256 MiB minus the 128 MiB arena minus slack
}
SCENARIOS = {
    # name: (load steps, sink capacity steps)
    'smoke':    ('20s:5000,20s:20000', '1h:inf'),
    'ramp':     ('30s:20000,30s:40000,30s:60000,30s:80000,30s:100000,30s:125000,30s:150000,30s:175000,30s:200000', '1h:inf'),
    'slowdown': ('210s:40000', '45s:inf,90s:24000,1h:inf'),
    'outage':   ('150s:40000', '45s:inf,20s:0,1h:inf'),
}

SAMPLER = r'''
cid=$(docker inspect -f '{{.Id}}' prioab-agent); cg=/sys/fs/cgroup/system.slice/docker-$cid.scope
echo "t usage_usec nr_throttled throttled_usec memory_current anon file oom_kill"
while [ -d $cg ]; do
  printf "%s %s %s %s %s %s %s %s\n" $(date +%s.%N) \
    $(awk '$1=="usage_usec"||$1=="nr_throttled"||$1=="throttled_usec"{print $2}' $cg/cpu.stat) \
    $(cat $cg/memory.current) $(awk '$1=="anon"||$1=="file"{print $2}' $cg/memory.stat) \
    $(awk '$1=="oom_kill"{print $2}' $cg/memory.events)
  sleep 1
done
'''


def ssh(node, cmd, check=True, timeout=None):
    return subprocess.run(['ssh', '-o', 'BatchMode=yes', node, cmd], check=check, text=True,
                          capture_output=True, timeout=timeout)


def cleanup():
    ssh(AGENT, 'docker rm -f prioab-agent >/dev/null 2>&1; pkill -f prioab-sampler; true', check=False)
    ssh(SINK, 'docker rm -f prioab-sink >/dev/null 2>&1; true', check=False)
    ssh(LOAD, 'docker rm -f prioab-load >/dev/null 2>&1; true', check=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('variant', choices=sorted(ENV))
    ap.add_argument('scenario', choices=sorted(SCENARIOS))
    ap.add_argument('--out', required=True)
    ap.add_argument('--hp-fraction', default='0.5')
    ap.add_argument('--drain', type=int, default=20, help='seconds to wait after the load for the collector to drain')
    args = ap.parse_args()
    load_steps, sink_steps = SCENARIOS[args.scenario]
    out = pathlib.Path(args.out) / args.variant
    out.mkdir(parents=True, exist_ok=False)

    cleanup()
    env = ' '.join(f'-e {k}={v}' for k, v in {**ENV[args.variant], 'GOGC': '100', 'GOMAXPROCS': '1',
                                              'GODEBUG': 'gctrace=1'}.items())
    ssh(AGENT, f'docker run -d --name prioab-agent --network host --cpus 0.5 --memory 256m --memory-swap 256m '
               f'{env} -v ~/prioab:/ab:ro --entrypoint /ab/prioritycol {IMAGE} --config /ab/{args.variant}.yaml')
    for _ in range(40):
        if ssh(AGENT, 'ss -ltn | grep -q ":14317 "', check=False).returncode == 0:
            break
        time.sleep(0.5)
    else:
        sys.exit('collector did not start: ' + ssh(AGENT, 'docker logs prioab-agent 2>&1 | tail -20', check=False).stdout)
    subprocess.Popen(['ssh', '-o', 'BatchMode=yes', AGENT,
                      f"setsid bash -c 'exec -a prioab-sampler bash' <<'EOF' > ~/prioab/out/cgroup.tsv 2>&1 &\n{SAMPLER}\nEOF"])
    ssh(SINK, f'docker run -d --name prioab-sink --network host -v ~/prioab:/ab:ro --entrypoint /ab/sink {IMAGE} '
              f'-listen 0.0.0.0:14317 -capacity {sink_steps} -max-queue 1s')
    t0 = time.time()
    load = ssh(LOAD, f'docker run --rm --name prioab-load --network host --cpus 6 -v ~/prioab:/ab:ro '
                     f'--entrypoint /ab/load {IMAGE} -endpoint {AGENT_IP}:14317 -steps {load_steps} '
                     f'-hp-fraction {args.hp_fraction} -clients 16', check=False)
    (out / 'load.jsonl').write_text(load.stdout)
    (out / 'load.stderr').write_text(load.stderr)
    time.sleep(args.drain)
    (out / 'agent.log').write_text(ssh(AGENT, 'docker logs prioab-agent 2>&1', check=False).stdout)
    state = ssh(AGENT, "docker inspect -f '{{json .State}}' prioab-agent", check=False).stdout
    (out / 'sink.jsonl').write_text(ssh(SINK, 'docker logs prioab-sink 2>/dev/null', check=False).stdout)
    ssh(AGENT, 'docker stop -t 10 prioab-agent >/dev/null', check=False)
    time.sleep(2)
    (out / 'agent-final.log').write_text(ssh(AGENT, 'docker logs --tail 40 prioab-agent 2>&1', check=False).stdout)
    (out / 'cgroup.tsv').write_text(ssh(AGENT, 'cat ~/prioab/out/cgroup.tsv', check=False).stdout)
    json.dump({'variant': args.variant, 'scenario': args.scenario, 'load_steps': load_steps,
               'sink_steps': sink_steps, 'hp_fraction': args.hp_fraction, 'env': ENV[args.variant],
               'load_rc': load.returncode, 'started': t0, 'agent_state': state.strip()},
              open(out / 'meta.json', 'w'), indent=1)
    cleanup()
    print(f'{args.variant} {args.scenario}: load rc {load.returncode}, results in {out}')


if __name__ == '__main__':
    main()
