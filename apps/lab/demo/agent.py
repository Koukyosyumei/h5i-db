"""A scripted demo agent. Not a language model: each role is a fixed plan.

The lab starts it with LAB_TOKEN (a one-day agent token) and LAB_AGENT in
the environment. Arguments: role, lab URL, decision time in nanoseconds.
Everything it does goes through the lab's HTTP API, so the protocol applies
to it as to any other agent: it can pre-register, request trials, freeze a
counted trial, and submit it to the holdout, and it cannot see the sealed
span or record its own results.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

ROLE, URL, DECISION = sys.argv[1], sys.argv[2].rstrip("/"), int(sys.argv[3])
TOKEN = os.environ["LAB_TOKEN"]
DAY = 86_400 * 10**9

CROSSOVER = """# moving-average crossover, long only
universe ALFA DLTA
let fast = sma($fast)
let slow = sma($slow)
long when fast > slow
flat when fast < slow
size $units
"""

REVERSION = """# buy a stretch below the rolling mean, sell on the way back
universe BRVO
let z = zscore($window)
long when z < -$entry
flat when z > $exit
size $units
"""

BREAKOUT = """# channel breakout
universe CHRL
long when mid >= high($entry)
flat when mid <= low($exit)
size $units
"""

PLANS = {
    "trend": {
        "hypothesis": "Hourly drift in ALFA and DLTA persists for weeks; a slow moving-average crossover rides it.",
        "method": "moving-average crossover",
        "name": "ma-cross",
        "code": CROSSOVER,
        "params": [
            {"fast": 12, "slow": 72, "units": 150},
            {"fast": 24, "slow": 168, "units": 150},
            {"fast": 6, "slow": 48, "units": 150},
        ],
        "freeze": 0,
        "holdout": False,
    },
    "reversion": {
        "hypothesis": "BRVO is pulled back to its two-day mean; buying 1.5 standard deviations below it pays after the spread.",
        "method": "z-score reversion",
        "name": "zrev",
        "code": REVERSION,
        "params": [
            {"window": 48, "entry": 1.5, "exit": 0, "units": 400},
            {"window": 24, "entry": 2.0, "exit": 0.5, "units": 400},
            {"window": 96, "entry": 1.0, "exit": 0, "units": 400},
        ],
        "freeze": 0,
        "holdout": True,
    },
    "breakout": {
        "hypothesis": "A new five-day high in CHRL starts a run that lasts until a two-day low.",
        "method": "channel breakout",
        "name": "donchian",
        "code": BREAKOUT,
        "params": [
            {"entry": 120, "exit": 48, "units": 300},
            {"entry": 72, "exit": 24, "units": 300},
        ],
        "freeze": None,
        "holdout": False,
        "peek": True,
    },
}


def call(path, body=None, key=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(URL + path, data=data, method="POST" if data else "GET")
    request.add_header("authorization", "Bearer " + TOKEN)
    request.add_header("content-type", "application/json")
    if key:
        request.add_header("idempotency-key", key)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def rpc(cmd, key=None):
    status, body = call("/rpc", cmd, key)
    if status != 200:
        raise SystemExit(f"{cmd['cmd']} refused: {status} {body}")
    return body


def say(text):
    print(f"[{ROLE}] {text}", flush=True)


def main():
    plan = PLANS[ROLE]
    agent = os.environ.get("LAB_AGENT", "?")
    family = rpc(
        {"cmd": "pre_register", "hypothesis": plan["hypothesis"], "method": plan["method"]},
        key=f"pre-{agent}",
    )["id"]
    say(f"pre-registered family {family}: {plan['method']}")
    trials = []
    for index, params in enumerate(plan["params"]):
        trial = rpc(
            {
                "cmd": "request_trial",
                "family": family,
                "name": plan["name"],
                "code": plan["code"],
                "params": params,
                "decision_time": DECISION,
            },
            key=f"trial-{agent}-{index}",
        )["id"]
        trials.append(trial)
        say(f"requested trial {trial} with {json.dumps(params, sort_keys=True)}")
    # The agent sees the family's trial count and corrected score, not raw results.
    for _ in range(600):
        report = rpc({"cmd": "report", "family": family})
        if report["trials"] >= len(trials):
            break
        time.sleep(0.5)
    say(f"{report['trials']} trials counted, deflated Sharpe {report['corrected'] / 1e4:.1f}%")
    if plan.get("peek"):
        # Asking for data past the decision time it registered is refused,
        # and the lab files a lookahead alert.
        status, body = call(
            "/api/query",
            {"sql": "SELECT count(*) AS n FROM book_deltas", "decision_time_ns": DECISION + 20 * DAY},
        )
        say(f"query 20 days past my decision time: {status} {body.get('error', '')}")
    if plan["freeze"] is not None:
        trial = trials[plan["freeze"]]
        rpc({"cmd": "freeze", "family": family, "trial": trial}, key=f"freeze-{agent}")
        say(f"froze trial {trial}")
        if plan["holdout"]:
            rpc({"cmd": "submit_holdout", "family": family}, key=f"holdout-{agent}")
            say("submitted the frozen strategy to the holdout")
    say("done")


if __name__ == "__main__":
    main()
