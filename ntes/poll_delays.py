"""poll_delays.py — sweep NTES station boards into a single live-delay snapshot.

The trick that makes whole-network delay tracking affordable: one station board
lists every train arriving/departing there in a time window, each with its live
delay and cancellation flag. So sweeping a few hundred busy junctions covers most
of the moving mainline fleet in a few hundred polite requests — no need to query
trains one by one.

Stations are seeded from ntes/major_stations.json (the ~250 busiest junctions,
bundled). One sweep writes data/out/delays.json:

  { "updatedAt": <epoch>, "source": "ntes-station-boards",
    "trains": { "<number>": {"d": <delayMinutes>} | {"c": 1}, ... } }

Usage:
  python ntes/poll_delays.py                 # one sweep
  python ntes/poll_delays.py --loop          # sweep ~every 5 min forever
  python ntes/poll_delays.py --stations 200 --hours 4
"""
import argparse
import json
import os
import random
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ntes import NTESClient
from ntes.exceptions import NTESError

ROOT = Path(__file__).resolve().parent.parent
SEED = Path(__file__).resolve().parent / "major_stations.json"
OUT = ROOT / "data" / "out" / "delays.json"
STATUS = ROOT / "data" / "out" / "poller-status.json"

IST = timezone(timedelta(hours=5, minutes=30))
VALID_HOURS = {2, 4, 8}          # NTES only accepts these look-ahead windows
MAX_BELIEVABLE_DELAY = 720       # min; beyond ~12h a "delay" is a stale board entry
PAUSE = 1.2
ATTEMPTS = 4


def log(*a):
    print(datetime.now(IST).strftime("%H:%M:%S"), *a, flush=True)


def parse_delay(s):
    """'HH:MM' -> minutes; anything else -> 0."""
    if not s or ":" not in str(s):
        return 0
    try:
        h, m = str(s).split(":")[:2]
        return max(0, int(h) * 60 + int(m))
    except ValueError:
        return 0


def parse_sched(s, now):
    """'15:08 10-Jul' -> aware datetime (current year), handling year rollover."""
    try:
        dt = datetime.strptime(f"{s}-{now.year}", "%H:%M %d-%b-%Y").replace(tzinfo=IST)
        if (dt - now).days > 180:
            dt = dt.replace(year=now.year - 1)
        if (now - dt).days > 180:
            dt = dt.replace(year=now.year + 1)
        return dt
    except ValueError:
        return None


def make_client():
    """Use a real connect/read timeout and one reusable pooled session."""
    try:
        client = NTESClient(timeout=30, retries=0)
    except TypeError:  # compatibility with older ntes-client releases
        client = NTESClient()
        if hasattr(client, "retries"):
            client.retries = 0
    client.timeout = (10, 30)
    try:
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
        client.session.mount("https://", adapter)
        client.session.mount("http://", adapter)
    except Exception as exc:
        log(f"connection-pool setup unavailable: {type(exc).__name__} — continuing")
    return client


def transient_ntes_error(exc):
    message = str(exc).lower()
    return any(marker in message for marker in (
        "request failed:", "empty response", "invalid json response", "timeout",
        "timed out", "connection", "temporarily", "502", "503", "504",
    ))


def station_call(client, code, hours):
    last = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            return client.station_live(code, hours), True
        except NTESError as exc:
            last = exc
            if not transient_ntes_error(exc):
                return None, False
        except Exception as exc:
            last = exc
        if attempt < ATTEMPTS:
            time.sleep(min(60, (5, 25, 60)[min(attempt - 1, 2)]) + random.uniform(0, 2))
    log(f"  {code}: {type(last).__name__}: {last} — skipped after retries")
    return None, False


def sweep(client, codes, hours):
    now = datetime.now(IST)
    best = {}  # number -> (proximity_seconds, delay_min, cancelled) — keep the nearest sighting
    calls = ok = 0
    for code in codes:
        r, answered = station_call(client, code, hours)
        if answered:
            ok += 1
        calls += 1
        time.sleep(PAUSE + random.uniform(0, 0.3))
        if not r:
            continue
        for t in r.get("TrainsAtStation", []) or []:
            no = str(t.get("TrainNumber", "")).strip()
            if not no:
                continue
            cancelled = bool(t.get("Cancel")) or bool(t.get("DepCancelFlag")) or bool(t.get("ArrCancelFlag"))
            delay = max(parse_delay(t.get("DelayArr")), parse_delay(t.get("DelayDep")))
            sched = parse_sched(t.get("STA") or t.get("STD") or "", now)
            prox = abs((sched - now).total_seconds()) if sched else 9e9
            cur = best.get(no)
            if cur is None or prox < cur[0]:
                best[no] = (prox, delay, cancelled)

    trains = {}
    for no, (_, d, c) in best.items():
        if c:
            trains[no] = {"c": 1}
        elif 5 <= d <= MAX_BELIEVABLE_DELAY:
            trains[no] = {"d": d}
    return trains, calls, ok


def publish(trains):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updatedAt": int(time.time()), "source": "ntes-station-boards", "trains": trains}
    fd, name = tempfile.mkstemp(prefix=f".{OUT.name}.", suffix=".tmp", dir=str(OUT.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(name, OUT)  # atomic — readers never see a half-written file
    except Exception:
        try:
            os.unlink(name)
        except OSError:
            pass
        raise


def main():
    ap = argparse.ArgumentParser(description="Sweep NTES station boards into one delays.json")
    ap.add_argument("--stations", type=int, default=150, help="how many top junctions to sweep")
    ap.add_argument("--hours", type=int, default=4, choices=sorted(VALID_HOURS))
    ap.add_argument("--loop", action="store_true", help="keep sweeping ~every 5 minutes")
    args = ap.parse_args()

    seed = json.loads(SEED.read_text())
    codes = [s["code"] for s in seed[: args.stations]]
    client = make_client()
    log(f"poller: {len(codes)} stations, window +/-{args.hours}h, loop={args.loop}")

    while True:
        t0 = time.time()
        trains, calls, ok = sweep(client, codes, args.hours)
        publish(trains)
        late = sum(1 for v in trains.values() if "d" in v)
        canc = sum(1 for v in trains.values() if "c" in v)
        dur = int(time.time() - t0)
        log(f"sweep done in {dur}s: {calls} calls ({ok} ok) -> {late} late, {canc} cancelled")
        status_payload = {
            "at": datetime.now(IST).isoformat(), "calls": calls, "ok": ok,
            "late": late, "cancelled": canc, "sweepSeconds": dur,
        }
        fd, name = tempfile.mkstemp(prefix=f".{STATUS.name}.", suffix=".tmp", dir=str(STATUS.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(status_payload, fh, indent=1)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(name, STATUS)
        except Exception:
            try:
                os.unlink(name)
            except OSError:
                pass
            raise
        if not args.loop:
            break
        time.sleep(max(10, 300 - (time.time() - t0)))


if __name__ == "__main__":
    main()
