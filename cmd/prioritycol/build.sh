#!/bin/bash
# Tomislav-RetCtx: build bin/prioritycol (see builder-config.yaml). The builder resolves some core
# modules to released versions older than the pre-release pdata contrib uses; they are pinned
# to the versions cmd/otelcontribcol resolves, then the binary is compiled.
set -euo pipefail
cd "$(dirname "$0")/../.."
.tools/builder --skip-compilation --config cmd/prioritycol/builder-config.yaml
cd cmd/prioritycol
python3 - <<'PY'
import re, subprocess
def reqs(path):
    return {m.group(1): m.group(2) for line in open(path)
            if (m := re.match(r'\s*(go\.opentelemetry\.io/collector\S*)\s+(v\S+)', line))}
full, slim = reqs("../otelcontribcol/go.mod"), reqs("go.mod")
pins = [f"-require={m}@{full[m]}" for m, v in slim.items() if m in full and full[m] != v]
if pins:
    subprocess.check_call(["go", "mod", "edit", *pins])
PY
CGO_ENABLED=0 go build -mod=mod -trimpath -o ../../bin/prioritycol .
echo "built bin/prioritycol"
