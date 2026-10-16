#!/usr/bin/env bash
# Run the lab on synthetic market data with three scripted agents.
#
#   apps/lab/demo.sh        build, start PostgreSQL and the lab, run the agents
#   apps/lab/demo.sh down   stop the lab and remove the PostgreSQL container
#
# The agents are scripts (demo/agent.py), not language models. Every number
# the page shows comes from backtests the worker ran on the generated data.
#
# Settings (environment):
#   DEMO_DIR        working directory: database, logs, token  (default: a new temp dir)
#   DEMO_PG_NAME    PostgreSQL container                       (default: h5idb-demo-pg)
#   DEMO_PG_PORT    its host port                              (default: 5470)
#   DEMO_LISTEN     where the lab listens                      (default: 127.0.0.1:8202)
#   DEMO_PG_ARGS    extra `docker run` flags, e.g. --cpuset-cpus=4-7
#   CARGO_TARGET_DIR                                           (default: apps/lab/target)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PG_NAME="${DEMO_PG_NAME:-h5idb-demo-pg}"
PG_PORT="${DEMO_PG_PORT:-5470}"
LISTEN="${DEMO_LISTEN:-127.0.0.1:8202}"
URL="http://$LISTEN"
STATE="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}/h5idb-lab-demo.dir"

if [ "${1:-}" = "down" ]; then
  if [ -f "$STATE" ]; then
    dir="$(cat "$STATE")"
    if [ -f "$dir/server.pid" ]; then
      kill "$(cat "$dir/server.pid")" 2>/dev/null || true
    fi
    rm -f "$STATE"
  fi
  docker rm -f "$PG_NAME" >/dev/null 2>&1 || true
  echo "stopped"
  exit 0
fi

DIR="${DEMO_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/h5idb-lab-demo.XXXXXX")}"
mkdir -p "$DIR"
echo "$DIR" > "$STATE"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HERE/target}"
BIN="$CARGO_TARGET_DIR/release"

echo "== build"
(cd "$HERE" && cargo build --release -p lab-server --bins)

echo "== PostgreSQL ($PG_NAME on 127.0.0.1:$PG_PORT)"
docker rm -f "$PG_NAME" >/dev/null 2>&1 || true
docker run -d --name "$PG_NAME" ${DEMO_PG_ARGS:-} -e POSTGRES_USER=lab -e POSTGRES_PASSWORD=lab \
  -e POSTGRES_DB=lab -p "127.0.0.1:$PG_PORT:5432" postgres:16 >/dev/null
for _ in $(seq 1 120); do
  docker exec "$PG_NAME" pg_isready -U lab -d lab >/dev/null 2>&1 && break
  sleep 0.5
done

echo "== market data (h5i-db at $DIR/market)"
rm -rf "$DIR/market"
"$BIN/lab-demo-data" "$DIR/market" 180 30 > "$DIR/market.json"
SEAL_START="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["seal_start_ns"])' "$DIR/market.json")"

# The signing secret and the human token stay in files readable by this user.
umask 077
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$DIR/secret"
export DATABASE_URL="postgres://lab:lab@127.0.0.1:$PG_PORT/lab"
export H5I_DB_PATH="$DIR/market"
export LISTEN LAB_SEAL=holdout LAB_TABLE=book_deltas
H5I_APP_SECRET="$(cat "$DIR/secret")" H5I_APP_ISSUE=1:0:1 "$BIN/lab-server" > "$DIR/human.token"
umask 022

echo "== lab ($URL)"
H5I_APP_SECRET="$(cat "$DIR/secret")" RUST_LOG="${RUST_LOG:-info}" \
  setsid "$BIN/lab-server" > "$DIR/server.log" 2>&1 &
echo $! > "$DIR/server.pid"
for _ in $(seq 1 120); do
  curl -sf "$URL/healthz" >/dev/null && break
  sleep 0.5
done

human() {
  curl -sf -H "authorization: Bearer $(cat "$DIR/human.token")" \
    -H 'content-type: application/json' "$@"
}

echo "== enroll three agents"
human -d '{"cmd":"enroll","agent":1,"budget":6,"token_budget":0,"sandbox":0,"name":"trend-follower"}' "$URL/rpc" >/dev/null
human -d '{"cmd":"enroll","agent":2,"budget":6,"token_budget":0,"sandbox":0,"name":"mean-reverter"}' "$URL/rpc" >/dev/null
human -d '{"cmd":"enroll","agent":3,"budget":4,"token_budget":0,"sandbox":0,"name":"breakout"}' "$URL/rpc" >/dev/null

echo "== start the agents (data up to the seal, $SEAL_START)"
agent=1
for role in trend reversion breakout; do
  printf 'exec python3 %s %s %s %s\n' "$HERE/demo/agent.py" "$role" "$URL" "$SEAL_START" > "$DIR/agent-$agent.sh"
  human -d "{\"agent\":$agent,\"script\":\"$DIR/agent-$agent.sh\",\"data_dir\":\"$DIR/market\"}" \
    "$URL/api/agents/start" >/dev/null
  agent=$((agent + 1))
done

fleet() { human -d '{"cmd":"fleet"}' "$URL/rpc"; }
field() { python3 -c "import json,sys; f=json.load(sys.stdin); $1"; }

echo "== wait for the trials and the holdout"
for _ in $(seq 1 600); do
  done_now="$(fleet | field 'fam=[x for x in f["families"] if x["agent"]==2]; print(int(len(f["trials"])>=8 and bool(fam) and fam[0]["holdout"] in (1,2)))')"
  [ "$done_now" = 1 ] && break
  sleep 1
done

echo "== the human reviews"
# Promote the reversion family if its holdout passed; leave a note on the trend family.
fleet | field '
for x in f["families"]:
    print(x["id"], x["agent"], x["holdout"])' | while read -r family agent holdout; do
  if [ "$agent" = 2 ] && [ "$holdout" = 1 ]; then
    human -d "{\"cmd\":\"promote\",\"family\":$family}" "$URL/rpc" >/dev/null && echo "promoted family $family"
  fi
  if [ "$agent" = 1 ]; then
    human -d "{\"family\":$family,\"decision\":\"note\",\"text\":\"Thin in-sample edge. The holdout went to the reversion family; this waits for the next seal.\"}" \
      "$URL/api/comment" >/dev/null && echo "commented on family $family"
  fi
done

cat <<EOF

The lab is at $URL
Human token: $DIR/human.token (paste it into the page; it is valid for a day)
Server log:  $DIR/server.log
Stop:        $HERE/demo.sh down
EOF
