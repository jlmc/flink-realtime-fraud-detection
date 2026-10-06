#!/bin/bash
# Produces sample transactions to Kafka (topic transaction.events) through the Kafka container: no local Kafka tools needed.
#
#   scripts/send-transactions.sh [options] [scenario ...]        (default: all)
#
# Scenarios (what you should see in transaction.risk.events / transaction.invalid.events / PostgreSQL):
#   normal        1 transaction                                   -> risk LOW, score 0
#   velocity      6 transactions of 1 EUR within 25 s             -> 6th: HIGH_TRANSACTION_VELOCITY
#   spending      3 x 2000 EUR within 40 s                        -> 3rd: HIGH_SPENDING_VELOCITY
#   country       PT, then US, then PT within 2 min               -> 3rd: SUSPICIOUS_COUNTRY_CHANGE
#   anomaly       25, 30, 20 EUR, then 900 EUR                    -> 4th: UNUSUAL_AMOUNT
#   invalid       negative amount, unsupported currency, missing fields, malformed JSON
#                                                                 -> 4 records in transaction.invalid.events, none processed
#   duplicate     the same transaction sent 3 times               -> exactly one result and one row
#   out-of-order  events with timestamps +20 s, +0 s, +10 s       -> evaluated in event-time order (0, 10, 20)
#   late          after the watermark moved on, an old event      -> stored with status LATE, not risk-evaluated
#
# Options:
#   --dry-run     print the messages instead of sending them
#   --wait N      seconds the "late" scenario waits for the watermark to move (default 45)
#   -h, --help
#
# Event time. Results appear once the watermark passes an event (event time + 30 s). Every scenario therefore gets its own
# event-time window, always later than the previous one (the cursor lives in .data/send-transactions.cursor), so repeated runs
# never produce accidental "late" events. After the scenarios a few events far in the future push the watermark forward.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }

DRY_RUN=0; WAIT=45; SCENARIOS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --wait) WAIT="$2"; shift 2 ;;
    -h|--help) sed -n '2,29p' "$0"; exit 0 ;;
    -*) echo "unknown option: $1 (see --help)" >&2; exit 1 ;;
    *) SCENARIOS+=("$1"); shift ;;
  esac
done
[ ${#SCENARIOS[@]} -gt 0 ] || SCENARIOS=(all)

ROOT="$ROOT" DRY_RUN="$DRY_RUN" WAIT="$WAIT" python3 - ${SCENARIOS[@]+"${SCENARIOS[@]}"} <<'PY'
import datetime, json, os, subprocess, sys, time

ROOT, DRY, WAIT = os.environ["ROOT"], os.environ["DRY_RUN"] == "1", int(os.environ["WAIT"])
CURSOR = os.path.join(ROOT, ".data", "send-transactions.cursor")
ORDER = ["normal", "velocity", "spending", "country", "anomaly", "invalid", "duplicate", "out-of-order", "late"]

requested = sys.argv[1:]
if "all" in requested:
    requested = ORDER
unknown = [s for s in requested if s not in ORDER]
if unknown:
    sys.exit(f"unknown scenario(s): {', '.join(unknown)}. Choose from: all, {', '.join(ORDER)}")
requested = [s for s in ORDER if s in requested]  # always in the order that keeps event time ascending


def iso(epoch):
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_cursor():
    try:
        return float(open(CURSOR).read().strip())
    except (OSError, ValueError):
        return 0.0


def write_cursor(value):
    if DRY:
        return
    os.makedirs(os.path.dirname(CURSOR), exist_ok=True)
    open(CURSOR, "w").write(str(value))


def send(lines, label):
    if DRY:
        print(f"--- {label}")
        print("\n".join(lines))
        return
    subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", "localhost:19092", "--topic", "transaction.events"],
        input="\n".join(lines) + "\n", text=True, cwd=ROOT, check=True)
    print(f"sent {len(lines):>2} message(s): {label}")


run = time.strftime("%H%M%S")
clock = max(time.time(), read_cursor() + 60)  # event time of the next window


def tx(tid, customer, amount, at, currency="EUR", country="PT"):
    return json.dumps({"transactionId": tid, "customerId": customer, "merchantId": "merchant-10", "amount": amount,
                       "currency": currency, "country": country, "timestamp": iso(at)}, separators=(",", ":"))


def window(span):
    """Reserve [clock, clock+span] for a scenario and move the clock past it (the watermark trails by 30 s)."""
    global clock
    start = clock
    clock = start + span + 60
    return start


def pusher(at, count=6):
    return [tx(f"push-{run}-{i}", f"watermark-{run}-{i}", 1, at) for i in range(count)]


flushed_by_late = False
for name in requested:
    cust = f"{name}-{run}"
    if name == "normal":
        t = window(0)
        send([tx(f"normal-{run}-1", cust, 25.00, t)], name)
    elif name == "velocity":
        t = window(25)
        send([tx(f"velocity-{run}-{i}", cust, 1, t + 5 * (i - 1)) for i in range(1, 7)], name)
    elif name == "spending":
        t = window(40)
        send([tx(f"spending-{run}-{i}", cust, 2000, t + 20 * (i - 1)) for i in range(1, 4)], name)
    elif name == "country":
        t = window(120)
        send([tx(f"country-{run}-1", cust, 40, t, country="PT"),
              tx(f"country-{run}-2", cust, 40, t + 60, country="US"),
              tx(f"country-{run}-3", cust, 40, t + 120, country="PT")], name)
    elif name == "anomaly":
        t = window(30)
        send([tx(f"anomaly-{run}-1", cust, 25, t), tx(f"anomaly-{run}-2", cust, 30, t + 10),
              tx(f"anomaly-{run}-3", cust, 20, t + 20), tx(f"anomaly-{run}-4", cust, 900, t + 30)], name)
    elif name == "invalid":
        t = window(10)
        send([tx(f"invalid-{run}-negative", cust, -5, t),
              tx(f"invalid-{run}-currency", cust, 10, t + 1, currency="JPY"),
              json.dumps({"transactionId": f"invalid-{run}-fields", "amount": 10}),
              "this is not json {"], name)
    elif name == "duplicate":
        t = window(0)
        send([tx(f"duplicate-{run}-1", cust, 15, t)] * 3, name)
    elif name == "out-of-order":
        t = window(20)
        send([tx(f"ooo-{run}-c", cust, 5, t + 20), tx(f"ooo-{run}-a", cust, 5, t),
              tx(f"ooo-{run}-b", cust, 5, t + 10)], name + " (arrival order: +20 s, +0 s, +10 s)")
    elif name == "late":
        t = window(0)
        far = t + 900  # far enough ahead that the late event below is clearly behind the watermark
        send(pusher(far), "late: events far in the future to move the watermark")
        if not DRY:
            print(f"waiting {WAIT}s for the watermark to reach all parallel subtasks (idle ones are skipped after 30 s)...")
            time.sleep(WAIT)
        send([tx(f"late-{run}-1", cust, 12, t)], "late: an event older than the watermark")
        clock = far + 60
        flushed_by_late = True

if not flushed_by_late:
    send(pusher(clock + 60), "flush: events far enough ahead to move the watermark past everything above")
    clock += 120

write_cursor(clock)
print(f"\nrun id {run}. Results show up after the next checkpoint (about 10 s). Look at them with:")
print(f"  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic transaction.risk.events --from-beginning --isolation-level read_committed --timeout-ms 5000")
print(f"  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic transaction.invalid.events --from-beginning --timeout-ms 5000")
print(f"  docker compose exec postgres psql -U fraud -d fraud -c \"select transaction_id, status from transactions where transaction_id like '%-{run}-%' order by 1\"")
PY
