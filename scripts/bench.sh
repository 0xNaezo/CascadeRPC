#!/usr/bin/env bash
# Load-test the balancer: mock nodes + balancer + oha, one command.
#
#   scripts/bench.sh                          # 30s, 400 connections
#   DURATION=60s CONNECTIONS=500 scripts/bench.sh
#   PROFILE=1 scripts/bench.sh                # also sample the balancer with perf
#   scripts/bench.sh show                     # print the history table, run nothing
#   scripts/bench.sh report                   # open the last profile (perf report)
#   scripts/bench.sh flame                    # render bench/flame.svg from it
#
# Everything lands in bench/, which is gitignored: history.jsonl (one row per
# run, tagged with the commit it measured), last.json (raw oha output),
# perf.data, and the two server logs.
set -euo pipefail

cd "$(dirname "$0")/.."

BENCH_DIR=${BENCH_DIR:-bench}
HISTORY="$BENCH_DIR/history.jsonl"
LAST="$BENCH_DIR/last.json"
PERF_OUT="$BENCH_DIR/perf.data"

DURATION=${DURATION:-30s}
CONNECTIONS=${CONNECTIONS:-400}
CONFIG=${CONFIG_PATH:-config/balancer_config/mock_nodes.toml}
PROFILE=${PROFILE:-0}

PORTS=(3000 8891 8892 8893 8894 8895 8896)

print_history() {
    [ -s "$HISTORY" ] || { echo "no runs recorded yet"; return 0; }
    printf '%-16s %-14s %8s %6s %8s %8s %8s\n' TIME COMMIT RPS OK% P50 P95 P99
    tail -n "${ROWS:-10}" "$HISTORY" | jq -r \
        '[(.ts | sub("T"; " ") | sub(":\\d\\dZ$"; "")), .commit, (.rps|tostring),
          (.ok_pct|tostring), (.p50_ms|tostring), (.p95_ms|tostring), (.p99_ms|tostring)] | @tsv' |
        while IFS=$'\t' read -r ts c r ok p50 p95 p99; do
            printf '%-16s %-14s %8s %6s %8s %8s %8s\n' "$ts" "$c" "$r" "$ok" "$p50" "$p95" "$p99"
        done
}

case "${1:-}" in
    show)
        print_history
        exit 0
        ;;
    report)
        [ -f "$PERF_OUT" ] || { echo "no $PERF_OUT — run PROFILE=1 scripts/bench.sh first" >&2; exit 1; }
        exec perf report -i "$PERF_OUT" "${@:2}"
        ;;
    flame)
        [ -f "$PERF_OUT" ] || { echo "no $PERF_OUT — run PROFILE=1 scripts/bench.sh first" >&2; exit 1; }
        command -v inferno-collapse-perf >/dev/null || { echo "missing: inferno (cargo install inferno)" >&2; exit 1; }
        perf script -i "$PERF_OUT" | inferno-collapse-perf | inferno-flamegraph > "$BENCH_DIR/flame.svg"
        echo "$BENCH_DIR/flame.svg"
        exit 0
        ;;
    "")
        ;;
    *)
        echo "unknown argument: $1 (expected: show | report | flame)" >&2
        echo "PROFILE is an environment variable: PROFILE=1 scripts/bench.sh" >&2
        exit 1
        ;;
esac

for tool in oha curl jq; do
    command -v "$tool" >/dev/null || { echo "missing: $tool" >&2; exit 1; }
done
[ "$PROFILE" = 1 ] && { command -v perf >/dev/null || { echo "missing: perf" >&2; exit 1; }; }

# A stale balancer squatting on :3000 does not make this script fail — the new
# one dies on bind, the readiness probe answers from the old process, and oha
# happily measures whatever binary got there first.
if command -v ss >/dev/null; then
    listening=$(ss -ltnH 2>/dev/null | awk '{print $4}' | sed 's/.*://' | sort -u)
    for port in "${PORTS[@]}"; do
        if grep -qx "$port" <<<"$listening"; then
            echo "port $port is already in use — kill the stale process first:" >&2
            echo "  ss -ltnp | grep :$port" >&2
            exit 1
        fi
    done
fi

# Six figures of RPS from one host exhausts the default fd limit long before
# the balancer runs out of anything interesting.
ulimit -n 65535 2>/dev/null || true

if [ "$PROFILE" = 1 ]; then
    # Frame pointers keep perf's unwinding cheap and correct; --call-graph
    # dwarf copies 8 KB of stack per sample, which is not survivable at this
    # request rate. Separate target dir so the plain release cache stays warm —
    # and so a profiling build, which is not the binary that ships, never
    # produces a number that lands in the history.
    export CARGO_TARGET_DIR=target/profiling
    export RUSTFLAGS="-C force-frame-pointers=yes"
    bin=target/profiling/release
else
    bin=target/release
fi

mkdir -p "$BENCH_DIR"
cargo build --release --bin cascaderpc --bin mock_node

pids=()
cleanup() {
    [ ${#pids[@]} -gt 0 ] && kill "${pids[@]}" 2>/dev/null
    return 0
}
trap cleanup EXIT

"$bin/mock_node" > "$BENCH_DIR/mock.log" 2>&1 &
pids+=($!)

CONFIG_PATH="$CONFIG" "$bin/cascaderpc" > "$BENCH_DIR/balancer.log" 2>&1 &
balancer=$!
pids+=($balancer)

for _ in $(seq 50); do
    kill -0 "$balancer" 2>/dev/null || { echo "balancer exited:" >&2; tail -5 "$BENCH_DIR/balancer.log" >&2; exit 1; }
    curl -sf http://localhost:3000/health >/dev/null && break
    sleep 0.2
done
curl -sf http://localhost:3000/health >/dev/null || { echo "balancer never came up" >&2; exit 1; }

load=(oha -z "$DURATION" -c "$CONNECTIONS" --no-tui --output-format json
      -m POST -H 'content-type: application/json'
      -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
      http://localhost:3000/send-request)

if [ "$PROFILE" = 1 ]; then
    # perf samples $balancer for exactly as long as oha drives load.
    rm -f "$PERF_OUT"
    perf record -F 999 -g -o "$PERF_OUT" -p "$balancer" -- "${load[@]}" > "$LAST"
    [ -s "$PERF_OUT" ] || { echo "perf wrote no samples to $PERF_OUT" >&2; exit 1; }
else
    "${load[@]}" > "$LAST"
fi

echo
echo "HTTP status:"
jq -r '.statusCodeDistribution | to_entries[] | "  \(.key)  \(.value)"' "$LAST"

commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
[ -n "$(git status --porcelain 2>/dev/null)" ] && commit="$commit-dirty"

# oha's successRate counts transport-level success: a run answered entirely in
# HTTP 504 still reports 1.0. The share of 2xx is the number that means the
# balancer actually served the load.
row=$(jq -c \
    --arg ts "$(date -u +%FT%TZ)" \
    --arg commit "$commit" \
    --arg duration "$DURATION" \
    --argjson connections "$CONNECTIONS" \
    --argjson profiled "$PROFILE" \
    '(.statusCodeDistribution | to_entries | map(select(.key | startswith("2"))) | map(.value) | add // 0) as $ok
     | (.statusCodeDistribution | [.[]] | add // 0) as $total
     | {ts: $ts, commit: $commit, duration: $duration, connections: $connections,
        profiled: ($profiled == 1),
        rps: (.summary.requestsPerSec | round),
        ok_pct: (if $total == 0 then 0 else (($ok / $total) * 1000 | round) / 10 end),
        status: .statusCodeDistribution,
        p50_ms: ((.latencyPercentiles.p50 * 100000 | round) / 100),
        p95_ms: ((.latencyPercentiles.p95 * 100000 | round) / 100),
        p99_ms: ((.latencyPercentiles.p99 * 100000 | round) / 100)}' \
    "$LAST")

ok_pct=$(jq -r '.ok_pct' <<<"$row")

# A run the balancer did not actually serve is not a data point to compare
# against; keeping it out of the history is what makes the history readable.
if awk "BEGIN{exit !($ok_pct < 99)}"; then
    echo
    echo "only ${ok_pct}% of requests came back 2xx — not recording this run" >&2
    echo "balancer log:" >&2
    tail -15 "$BENCH_DIR/balancer.log" >&2
    exit 1
fi

echo "$row" >> "$HISTORY"
echo
print_history

[ "$PROFILE" = 1 ] && perf report -i "$PERF_OUT" --stdio --percent-limit 1 --no-children | head -40
exit 0
