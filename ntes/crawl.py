"""Resumable, rate-limited NTES timetable crawler.

The crawl has two stages:

* discovery searches the adaptive three-to-five digit prefix tree and persists
  both completed prefixes and the pending queue;
* schedules fetches each discovered train with bounded worker concurrency and
  writes each successful response atomically.

The network is still paced globally (the default is at most 50 request starts
per minute), so workers hide connection latency without turning the public NTES
service into a burst target. Every durable unit of work is independently
resumable. A transient failure is retained as retryable state; a semantic
"train not found" response is retained as a permanent failure unless
``--retry-errors`` is requested.

Outputs (relative to the repository root):

  data/raw/schedules/<number>.json   one validated NTES schedule per train
  data/raw/numbers.json              roster + discovery checkpoint
  data/raw/errors.json               structured schedule failures
  data/raw/crawl-status.json         live progress and health information
  data/runtime/crawler.lock         held while one crawler is active

Usage:
  python ntes/crawl.py
  python ntes/crawl.py --pause 1.2 --workers 4
  python ntes/crawl.py --seed roster.json --retry-errors
"""
import argparse
import json
import os
import random
import signal
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover - the supported deployment is POSIX
    fcntl = None

from ntes import NTESClient
from ntes.exceptions import NTESError


ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "raw"
SCHED = OUT / "schedules"
STATUS = OUT / "crawl-status.json"
NUMBERS = OUT / "numbers.json"
ERRORS = OUT / "errors.json"
RUNTIME = ROOT / "data" / "runtime"
CRAWLER_LOCK = RUNTIME / "crawler.lock"

CAP = 60                         # observed search-result cap
MIN_PAUSE = 0.6                  # safety floor; do not hammer the endpoint
DEFAULT_PAUSE = 1.2              # 50 request starts/minute before retries
DEFAULT_JITTER = 0.4
DEFAULT_WORKERS = 4
DEFAULT_ATTEMPTS = 4
DEFAULT_CHECKPOINT_EVERY = 25
DEFAULT_CHECKPOINT_SECONDS = 30.0
DEFAULT_HEARTBEAT_SECONDS = 15.0
DEFAULT_MAX_NETWORK_FAILURES = 12
EXIT_RETRYABLE = 3
EXIT_STALLED = 2

_status_store = None
_log_lock = threading.Lock()
_stop_event = threading.Event()
_client_local = threading.local()


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def log(message):
    """Write a line that remains useful when stdout is redirected by the manager."""
    with _log_lock:
        print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} {message}", flush=True)


def load_json(path, default):
    try:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def atomic_write_json(path, payload):
    """Write JSON durably and replace the visible file in one filesystem step."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


class StatusStore:
    """Small atomic status document shared with the watchdog/status command."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        initial = load_json(path, {})
        self.data = dict(initial) if isinstance(initial, dict) else {}

    def update(self, **fields):
        with self.lock:
            self.data.update(fields)
            self.data["updatedAt"] = iso_now()
            atomic_write_json(self.path, self.data)

    def snapshot(self):
        with self.lock:
            return dict(self.data)


def status(**fields):
    """Update status without making callers care whether main() is initialized."""
    if _status_store is not None:
        _status_store.update(**fields)
        return
    current = load_json(STATUS, {})
    if not isinstance(current, dict):
        current = {}
    current.update(fields)
    current["updatedAt"] = iso_now()
    atomic_write_json(STATUS, current)


class Heartbeat(threading.Thread):
    def __init__(self, store, interval):
        super().__init__(name="crawler-heartbeat", daemon=True)
        self.store = store
        self.interval = max(2.0, interval)
        self.done = threading.Event()

    def stop(self):
        self.done.set()

    def run(self):
        while not self.done.wait(self.interval):
            try:
                self.store.update(heartbeatAt=iso_now())
            except OSError as exc:
                log(f"status checkpoint failed: {type(exc).__name__}: {exc}")


@contextmanager
def crawler_lock():
    """Prevent an operator and the watchdog from running two crawlers."""
    if fcntl is None:
        raise RuntimeError("crawler locking requires a POSIX host with fcntl")
    RUNTIME.mkdir(parents=True, exist_ok=True)
    fh = CRAWLER_LOCK.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if getattr(exc, "errno", None) in (11, 35):
                fh.seek(0)
                owner = fh.read().strip() or "unknown"
                raise LockUnavailable(f"another crawler is already running ({owner})")
            raise
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid={os.getpid()}\n")
        fh.flush()
        os.fsync(fh.fileno())
        yield
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


@dataclass
class Config:
    pause: float = DEFAULT_PAUSE
    jitter: float = DEFAULT_JITTER
    workers: int = DEFAULT_WORKERS
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    attempts: int = DEFAULT_ATTEMPTS
    checkpoint_every: int = DEFAULT_CHECKPOINT_EVERY
    checkpoint_seconds: float = DEFAULT_CHECKPOINT_SECONDS
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS
    max_network_failures: int = DEFAULT_MAX_NETWORK_FAILURES
    retry_errors: bool = False
    limit: int = 0


class RateLimiter:
    """A single global request-start schedule shared by all worker threads."""

    def __init__(self, interval, jitter):
        self.interval = max(MIN_PAUSE, interval)
        self.jitter = max(0.0, jitter)
        self.lock = threading.Lock()
        self.next_start = 0.0

    def acquire(self, stop_event):
        with self.lock:
            now = time.monotonic()
            target = max(now, self.next_start)
            self.next_start = target + self.interval + random.uniform(0, self.jitter)
        while True:
            remaining = target - time.monotonic()
            if remaining <= 0:
                return not stop_event.is_set()
            if stop_event.wait(min(remaining, 1.0)):
                return False


class EndpointCircuit:
    """Stop spending hours retrying when the endpoint is clearly unavailable."""

    def __init__(self, threshold):
        self.threshold = max(1, threshold)
        self.lock = threading.Lock()
        self.network_failures = 0
        self.opened = False

    def before(self):
        with self.lock:
            return not self.opened

    def success(self):
        with self.lock:
            self.network_failures = 0

    def failure(self):
        with self.lock:
            self.network_failures += 1
            if self.network_failures >= self.threshold:
                self.opened = True

    def is_open(self):
        with self.lock:
            return self.opened


@dataclass
class CallResult:
    value: object = None
    ok: bool = False
    retryable: bool = False
    semantic: bool = False
    error: str = ""
    attempts: int = 0
    stalled: bool = False
    stopped: bool = False


class RetryableWork(Exception):
    pass


class StalledWork(Exception):
    pass


class LockUnavailable(RuntimeError):
    pass


def is_transient_ntes_error(exc):
    """Recover the distinction hidden by ntes-client's broad NTESError wrapper."""
    message = str(exc).strip().lower()
    transient_markers = (
        "request failed:",
        "empty response",
        "invalid json response",
        "timed out",
        "timeout",
        "connection",
        "temporarily",
        "503",
        "502",
        "504",
    )
    return any(marker in message for marker in transient_markers)


def retry_delay(attempt):
    # Delays between attempts are intentionally bounded and jittered.
    base = (5, 25, 60, 120, 300, 600)
    return base[min(max(attempt - 1, 0), len(base) - 1)] + random.uniform(0, 2)


def call(client_fn, *args, limiter, circuit, config, label):
    """Call one NTES operation with classified, interruptible retries."""
    last_error = ""
    for attempt in range(1, config.attempts + 1):
        if _stop_event.is_set():
            return CallResult(error="shutdown requested", attempts=attempt - 1, stopped=True)
        if not circuit.before():
            return CallResult(
                error="endpoint circuit open", attempts=attempt - 1,
                retryable=True, stalled=True
            )
        if not limiter.acquire(_stop_event):
            return CallResult(error="shutdown requested", attempts=attempt - 1, stopped=True)
        try:
            value = client_fn(*args)
        except NTESError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if not is_transient_ntes_error(exc):
                circuit.success()  # the endpoint answered a semantic request
                return CallResult(
                    error=last_error, attempts=attempt, semantic=True
                )
            circuit.failure()
            if attempt < config.attempts and not circuit.is_open():
                log(f"{label}: transient failure {attempt}/{config.attempts}; retrying: {last_error}")
                if _stop_event.wait(retry_delay(attempt)):
                    return CallResult(error="shutdown requested", attempts=attempt, stopped=True)
                continue
        except Exception as exc:  # requests/crypto/decode failures are transient by default
            last_error = f"{type(exc).__name__}: {exc}"
            circuit.failure()
            if attempt < config.attempts and not circuit.is_open():
                log(f"{label}: transient failure {attempt}/{config.attempts}; retrying: {last_error}")
                if _stop_event.wait(retry_delay(attempt)):
                    return CallResult(error="shutdown requested", attempts=attempt, stopped=True)
                continue
        else:
            circuit.success()
            return CallResult(value=value, ok=True, attempts=attempt)

        if circuit.is_open():
            break

    return CallResult(
        error=last_error or "request failed", attempts=config.attempts,
        retryable=True, stalled=circuit.is_open()
    )


def make_client(config):
    """Create one session per worker with reusable HTTP connections.

    ntes-client accepts an integer timeout but passes it directly to requests;
    requests also accepts a ``(connect, read)`` tuple, which is what we assign
    after construction so a silent socket cannot hang a worker indefinitely.
    """
    try:
        client = NTESClient(timeout=config.read_timeout, retries=0)
    except TypeError:  # compatibility with an older ntes-client constructor
        client = NTESClient()
        if hasattr(client, "retries"):
            client.retries = 0
    client.timeout = (config.connect_timeout, config.read_timeout)
    try:
        from requests.adapters import HTTPAdapter

        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
        client.session.mount("https://", adapter)
        client.session.mount("http://", adapter)
    except Exception as exc:  # the client remains usable if requests changes
        log(f"connection-pool setup unavailable: {type(exc).__name__}: {exc}")
    return client


def normalize_train_number(value):
    number = str(value).strip()
    return number if number.isdigit() else ""


def _prefix_state(raw):
    meta = raw.get("__prefixes__", {}) if isinstance(raw, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    done = set(str(p) for p in (meta.get("done") or []))
    pending = [str(p) for p in (meta.get("pending") or [])]
    queued = set(pending)
    base = [f"{i:03d}" for i in range(1000)]
    for prefix in base:
        if prefix not in done and prefix not in queued:
            pending.append(prefix)
            queued.add(prefix)
    raw_errors = meta.get("errors") or {}
    errors = {}
    if isinstance(raw_errors, dict):
        for prefix, record in raw_errors.items():
            if isinstance(record, dict):
                errors[str(prefix)] = dict(record)
            elif isinstance(record, str):
                errors[str(prefix)] = {
                    "message": record, "attempts": 0,
                    "lastAttemptAt": "", "retryAfter": time.time(),
                }
    return done, pending, errors, int(meta.get("swept", len(done)) or 0), int(meta.get("attempted", 0) or 0)


def _save_numbers(found, done, pending, prefix_errors, swept, attempted):
    payload = {str(k): v for k, v in found.items() if not str(k).startswith("__")}
    payload["__prefixes__"] = {
        "done": sorted(done),
        "pending": list(dict.fromkeys(pending)),
        "errors": prefix_errors,
        "swept": swept,
        "attempted": attempted,
    }
    atomic_write_json(NUMBERS, payload)


def discover(client, seed_numbers, config):
    raw = load_json(NUMBERS, {})
    if not isinstance(raw, dict):
        raw = {}
    found = {
        normalize_train_number(k): v for k, v in raw.items()
        if not str(k).startswith("__") and normalize_train_number(k)
    }
    if raw:
        log(f"resuming discovery over {len(found)} known numbers")
    for no in seed_numbers:
        value = normalize_train_number(no)
        if value and not value.startswith("__"):
            found.setdefault(value, {"src": "seed"})

    done, pending, prefix_errors, swept, attempted = _prefix_state(raw)
    deferred = []
    completed_this_run = 0
    checkpoint_units = 0
    last_checkpoint = time.monotonic()
    while pending:
        if _stop_event.is_set():
            _save_numbers(found, done, pending + deferred, prefix_errors, swept, attempted)
            raise RetryableWork("shutdown requested during discovery")
        prefix = pending.pop()
        if prefix in done:
            continue
        result = call(
            client.search, prefix, limiter=discover.limiter, circuit=discover.circuit,
            config=config, label=f"search {prefix}"
        )
        attempted += 1
        checkpoint_units += 1
        if result.stopped:
            _save_numbers(found, done, pending + deferred, prefix_errors, swept, attempted)
            raise RetryableWork(result.error)
        if result.stalled:
            _save_numbers(found, done, pending + deferred + [prefix], prefix_errors, swept, attempted)
            raise StalledWork(result.error)
        if result.retryable:
            previous = prefix_errors.get(prefix, {})
            attempts = int(previous.get("attempts", 0) or 0) + max(result.attempts, 1)
            prefix_errors[prefix] = {
                "message": result.error,
                "attempts": attempts,
                "lastAttemptAt": iso_now(),
                "retryAfter": time.time() + min(3600, retry_delay(attempts)),
            }
            deferred.append(prefix)
            log(f"search {prefix}: deferred after {result.attempts} attempts: {result.error}")
        else:
            prefix_errors.pop(prefix, None)
            trains = (result.value or {}).get("Trains", []) if isinstance(result.value, dict) else []
            trains = trains or []
            for train in trains:
                no = normalize_train_number(train.get("TrainNumber", ""))
                if no.startswith(prefix) and no not in found:
                    found[no] = {
                        "name": train.get("TrainName", ""),
                        "type": train.get("Type", ""),
                        "src": f"search:{prefix}",
                    }
            if len(trains) >= CAP and len(prefix) < 5:
                queued = set(pending) | set(deferred) | done
                for digit in "0123456789":
                    child = f"{prefix}{digit}"
                    if child not in queued:
                        pending.append(child)
                        queued.add(child)
            done.add(prefix)
            swept += 1
            completed_this_run += 1

        now = time.monotonic()
        if (
            checkpoint_units >= max(1, config.checkpoint_every)
            or now - last_checkpoint >= config.checkpoint_seconds
        ):
            _save_numbers(found, done, pending + deferred, prefix_errors, swept, attempted)
            last_checkpoint = now
            checkpoint_units = 0
            status(
                phase="discovery",
                stage="discovery",
                pid=os.getpid(),
                prefixesSwept=swept,
                prefixesAttempted=attempted,
                queue=len(pending) + len(deferred),
                numbersFound=len(found),
                total=len(found),
                completed=0,
                remaining=len(pending) + len(deferred),
                currentTask=f"searching prefix {prefix}",
                lastCheckpointAt=iso_now(),
            )
            log(
                f"discovery: swept={swept} queue={len(pending) + len(deferred)} "
                f"found={len(found)}"
            )

    if deferred:
        _save_numbers(found, done, deferred, prefix_errors, swept, attempted)
        retry_times = []
        for value in prefix_errors.values():
            try:
                retry_times.append(float(value.get("retryAfter", time.time() + 60)))
            except (AttributeError, TypeError, ValueError):
                retry_times.append(time.time() + 60)
        next_retry = min(retry_times, default=time.time() + 60)
        status(
            phase="RETRY_WAIT", stage="discovery", pid=os.getpid(),
            prefixesSwept=swept, prefixesAttempted=attempted, queue=len(deferred),
            numbersFound=len(found), remaining=len(deferred),
            retryableErrors=len(deferred), nextRetryAt=datetime.fromtimestamp(
                next_retry, timezone.utc
            ).replace(microsecond=0).isoformat(),
            lastError="one or more discovery prefixes need retry",
        )
        raise RetryableWork("discovery has retryable prefixes")

    _save_numbers(found, done, [], prefix_errors, swept, attempted)
    status(
        phase="discovery-done", stage="discovery", pid=os.getpid(),
        prefixesSwept=swept, prefixesAttempted=attempted, numbersFound=len(found),
        queue=0, lastCheckpointAt=iso_now(), currentTask="discovery complete",
    )
    log(f"discovery complete: {len(found)} numbers")
    return found


# Attributes are set once per crawl and deliberately shared by discovery only.
discover.limiter = None
discover.circuit = None


def normalize_errors(raw):
    """Read both the old string map and the new structured error map."""
    if not isinstance(raw, dict):
        return {}
    result = {}
    for no, record in raw.items():
        if str(no).startswith("__"):
            continue
        if isinstance(record, str):
            result[str(no)] = {
                "message": record,
                # The old format did not preserve the failure class. Retry it
                # once so a historical transient error is not lost forever;
                # the new response will be classified correctly.
                "kind": "legacy",
                "permanent": False,
                "attempts": 0,
                "lastAttemptAt": "",
                "retryAfter": 0,
            }
        elif isinstance(record, dict):
            result[str(no)] = dict(record)
    return result


def schedule_path(no):
    return SCHED / f"{no}.json"


def valid_schedule(path):
    try:
        with path.open(encoding="utf-8") as fh:
            value = json.load(fh)
        return isinstance(value, dict) and bool(value.get("stations"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return False


def write_schedule(no, value):
    """Commit a schedule only after the JSON is fully written and synced."""
    path = schedule_path(no)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{no}.", suffix=".json.tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(value, fh, ensure_ascii=False, separators=(",", ":"))
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def error_is_retryable(record, retry_errors):
    if retry_errors:
        return True
    return not bool(record.get("permanent")) and record.get("kind") != "semantic"


def retry_after_timestamp(record):
    try:
        return float(record.get("retryAfter", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


class Progress:
    def __init__(self, total, completed):
        self.lock = threading.Lock()
        self.total = total
        self.completed = completed
        self.attempted = 0
        self.successes = 0
        self.failures = 0
        self.transient_failures = 0
        self.permanent_failures = 0
        self.active = set()
        self.current = "scheduling work"
        self.started = time.monotonic()

    def begin(self, no):
        with self.lock:
            self.active.add(no)
            self.current = f"fetching schedule {no}"

    def end(self, no):
        with self.lock:
            self.active.discard(no)

    def result(self, no, success, retryable):
        with self.lock:
            self.attempted += 1
            self.failures += 0 if success else 1
            self.successes += 1 if success else 0
            self.transient_failures += 1 if (not success and retryable) else 0
            self.permanent_failures += 1 if (not success and not retryable) else 0
            if success:
                self.completed += 1
            self.current = f"completed schedule {no}" if success else f"handled schedule {no}"

    def snapshot(self):
        with self.lock:
            elapsed = max(time.monotonic() - self.started, 0.001)
            processed_rate = self.attempted / elapsed * 60.0
            success_rate = self.successes / elapsed * 60.0
            remaining = max(0, self.total - self.completed)
            eta = int(remaining / (success_rate / 60.0)) if success_rate > 0 else None
            return {
                "total": self.total,
                "completed": self.completed,
                "remaining": remaining,
                "attempted": self.attempted,
                "successes": self.successes,
                "failures": self.failures,
                "transientFailures": self.transient_failures,
                "permanentFailures": self.permanent_failures,
                "speedPerMinute": round(processed_rate, 2),
                "successSpeedPerMinute": round(success_rate, 2),
                "etaSeconds": eta,
                "activeTasks": sorted(self.active)[:20],
                "currentTask": self.current,
            }


def _worker_client(config):
    client = getattr(_client_local, "client", None)
    if client is None:
        client = make_client(config)
        _client_local.client = client
    return client


def _fetch_one(no, config, limiter, circuit, progress):
    progress.begin(no)
    try:
        client = _worker_client(config)
        return no, call(
            client.schedule, no, limiter=limiter, circuit=circuit,
            config=config, label=f"schedule {no}"
        )
    finally:
        progress.end(no)


def _error_record(result, previous, semantic=False, message=None):
    prior_attempts = int(previous.get("attempts", 0) or 0) if isinstance(previous, dict) else 0
    attempts = prior_attempts + max(result.attempts, 1)
    record = {
        "message": message or result.error or "empty-or-error",
        "kind": "semantic" if semantic else "transient",
        "permanent": bool(semantic),
        "attempts": attempts,
        "requestAttempts": result.attempts,
        "lastAttemptAt": iso_now(),
    }
    if not semantic:
        record["retryAfter"] = time.time() + min(3600, retry_delay(attempts))
    return record


def _status_progress(progress, **extra):
    payload = progress.snapshot()
    payload.update(extra)
    payload.setdefault("phase", "schedules")
    payload.setdefault("stage", "schedules")
    payload["pid"] = os.getpid()
    status(**payload)


def fetch_schedules(numbers, config):
    SCHED.mkdir(parents=True, exist_ok=True)
    errors = normalize_errors(load_json(ERRORS, {}))
    train_numbers = sorted({
        normalize_train_number(n) for n in numbers
        if not str(n).startswith("__") and normalize_train_number(n)
    })
    completed = sum(1 for no in train_numbers if valid_schedule(schedule_path(no)))
    now = time.time()
    todo = []
    deferred_retryable = []
    for no in train_numbers:
        if valid_schedule(schedule_path(no)):
            continue
        record = errors.get(no)
        if record and not error_is_retryable(record, config.retry_errors):
            continue
        if record and not config.retry_errors and retry_after_timestamp(record) > now:
            deferred_retryable.append(no)
            continue
        todo.append(no)
    if config.limit > 0:
        unprocessed = todo[config.limit:]
        todo = todo[:config.limit]
    else:
        unprocessed = []

    progress = Progress(len(train_numbers), completed)
    _status_progress(
        progress,
        phase="schedules", stage="schedules", workers=config.workers,
        queued=len(todo), failed=len(errors), retryableErrors=len(deferred_retryable),
        lastError="",
        startedAt=iso_now(), currentTask="starting schedule workers",
    )
    log(
        f"schedules: {len(todo)} ready ({len(deferred_retryable)} waiting for retry, "
        f"{completed}/{len(train_numbers)} already complete), workers={config.workers}"
    )

    limiter = RateLimiter(config.pause, config.jitter)
    circuit = EndpointCircuit(config.max_network_failures)
    futures = set()
    future_numbers = {}
    executor = None
    hard_stall = None
    processed_since_checkpoint = 0
    last_checkpoint = time.monotonic()
    last_status_write = last_checkpoint
    next_index = 0
    try:
        if todo:
            executor = ThreadPoolExecutor(max_workers=min(config.workers, len(todo)))
            in_flight_limit = min(len(todo), max(config.workers, config.workers * 2))
            for no in todo[:in_flight_limit]:
                future = executor.submit(_fetch_one, no, config, limiter, circuit, progress)
                futures.add(future)
                future_numbers[future] = no
            next_index = in_flight_limit
        while futures:
            if _stop_event.is_set():
                for future in futures:
                    future.cancel()
                break
            done, futures = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)
            if not done:
                if time.monotonic() - last_status_write >= 5.0:
                    _status_progress(progress, activeTasks=progress.snapshot()["activeTasks"])
                    last_status_write = time.monotonic()
                continue
            for future in done:
                no = future_numbers[future]
                try:
                    returned_no, result = future.result()
                    no = returned_no
                except Exception as exc:
                    # A worker bug must not lose the item or corrupt the checkpoint.
                    result = CallResult(
                        error=f"worker {type(exc).__name__}: {exc}",
                        retryable=True, attempts=1,
                    )
                previous = errors.get(no, {})
                if result.stopped:
                    errors[no] = _error_record(result, previous, message=result.error)
                    progress.result(no, False, True)
                    continue
                if result.stalled:
                    errors[no] = _error_record(result, previous, message=result.error)
                    progress.result(no, False, True)
                    hard_stall = result.error or "endpoint circuit open"
                    continue
                if result.ok and isinstance(result.value, dict) and result.value.get("stations"):
                    write_schedule(no, result.value)
                    errors.pop(no, None)
                    progress.result(no, True, False)
                elif result.ok:
                    errors[no] = _error_record(
                        result, previous, semantic=True, message="empty-or-error"
                    )
                    progress.result(no, False, False)
                else:
                    errors[no] = _error_record(result, previous, semantic=not result.retryable)
                    progress.result(no, False, result.retryable)
                    if result.error:
                        log(f"schedule {no}: {result.error}")
                processed_since_checkpoint += 1
                current = progress.snapshot()
                if (
                    processed_since_checkpoint >= max(1, config.checkpoint_every)
                    or time.monotonic() - last_checkpoint >= config.checkpoint_seconds
                ):
                    atomic_write_json(ERRORS, errors)
                    last_checkpoint = time.monotonic()
                    processed_since_checkpoint = 0
                    _status_progress(
                        progress,
                        phase="schedules", queued=len(futures) + len(todo) - next_index,
                        failed=len(errors), lastCheckpointAt=iso_now(),
                        lastError=(errors.get(no) or {}).get("message", ""),
                    )
                    last_status_write = time.monotonic()
                    log(
                        f"schedules: completed={current['completed']}/{current['total']} "
                        f"remaining={current['remaining']} speed={current['speedPerMinute']}/min"
                    )
            if not hard_stall and not _stop_event.is_set():
                refill = len(done)
                for no in todo[next_index:next_index + refill]:
                    future = executor.submit(_fetch_one, no, config, limiter, circuit, progress)
                    futures.add(future)
                    future_numbers[future] = no
                next_index += min(refill, len(todo) - next_index)
            if time.monotonic() - last_status_write >= 5.0:
                _status_progress(
                    progress, phase="schedules",
                    queued=len(futures) + len(todo) - next_index,
                )
                last_status_write = time.monotonic()
            if hard_stall:
                for future in futures:
                    future.cancel()
                break
    finally:
        if executor is not None:
            executor.shutdown(wait=True)
        atomic_write_json(ERRORS, errors)

    if _stop_event.is_set():
        _status_progress(progress, phase="STOPPED", lastCheckpointAt=iso_now())
        raise RetryableWork("shutdown requested")

    if hard_stall:
        _status_progress(
            progress, phase="STALLED", failed=len(errors),
            retryableErrors=sum(1 for v in errors.values() if not v.get("permanent")),
            lastError=hard_stall, lastCheckpointAt=iso_now(),
        )
        raise StalledWork(hard_stall)

    snapshot = progress.snapshot()
    retryable = [
        no for no, record in errors.items()
        if no in train_numbers and not valid_schedule(schedule_path(no))
        and error_is_retryable(record, False)
    ]
    retryable.extend(no for no in deferred_retryable if no not in retryable)
    retryable.extend(no for no in unprocessed if no not in retryable)
    if retryable:
        next_retry = min(
            (retry_after_timestamp(errors.get(no, {})) for no in retryable if errors.get(no)),
            default=time.time() + 10,
        )
        phase = "RETRY_WAIT" if not unprocessed else "PARTIAL"
        _status_progress(
            progress, phase=phase, failed=len(errors), retryableErrors=len(retryable),
            nextRetryAt=datetime.fromtimestamp(next_retry, timezone.utc).replace(
                microsecond=0
            ).isoformat(),
            lastCheckpointAt=iso_now(),
            lastError=(errors.get(retryable[0]) or {}).get("message", "retryable work remains"),
        )
        raise RetryableWork(f"{len(retryable)} schedule items need another run")

    permanent = sum(
        1 for no, record in errors.items()
        if no in train_numbers and not valid_schedule(schedule_path(no))
        and not error_is_retryable(record, False)
    )
    _status_progress(
        progress, phase="DONE", failed=len(errors), permanentFailures=permanent,
        retryableErrors=0, remaining=max(0, len(train_numbers) - snapshot["completed"]),
        lastCheckpointAt=iso_now(), currentTask="crawl complete",
    )
    log(
        f"crawl complete: {snapshot['completed']} schedules, "
        f"{permanent} permanent semantic failures"
    )
    return 0


def read_seed(path):
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        raise ValueError(f"seed file does not exist: {source}")
    raw = load_json(source, None)
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, dict):
        values = [key for key in raw if not str(key).startswith("__")]
    else:
        raise ValueError("seed JSON must be a list or object")
    return [number for value in values if (number := normalize_train_number(value))]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Polite, concurrent, resume-safe NTES crawler")
    parser.add_argument("--pause", type=float, default=_env_float("RAILPULL_PAUSE", DEFAULT_PAUSE),
                        help="minimum seconds between global request starts (default: 1.2)")
    parser.add_argument("--jitter", type=float, default=_env_float("RAILPULL_JITTER", DEFAULT_JITTER),
                        help="random extra pacing in seconds (default: 0.4)")
    parser.add_argument("--workers", type=int, default=_env_int("RAILPULL_WORKERS", DEFAULT_WORKERS),
                        help="bounded schedule workers; requests remain globally rate-limited")
    parser.add_argument("--connect-timeout", type=float,
                        default=_env_float("RAILPULL_CONNECT_TIMEOUT", 10.0))
    parser.add_argument("--read-timeout", type=float,
                        default=_env_float("RAILPULL_READ_TIMEOUT", 30.0))
    parser.add_argument("--attempts", type=int, default=_env_int("RAILPULL_ATTEMPTS", DEFAULT_ATTEMPTS),
                        help="attempts per request, including the first")
    parser.add_argument("--checkpoint-every", type=int,
                        default=_env_int("RAILPULL_CHECKPOINT_EVERY", DEFAULT_CHECKPOINT_EVERY))
    parser.add_argument("--checkpoint-seconds", type=float,
                        default=_env_float("RAILPULL_CHECKPOINT_SECONDS", DEFAULT_CHECKPOINT_SECONDS))
    parser.add_argument("--heartbeat-seconds", type=float,
                        default=_env_float("RAILPULL_HEARTBEAT_SECONDS", DEFAULT_HEARTBEAT_SECONDS))
    parser.add_argument("--max-network-failures", type=int,
                        default=_env_int("RAILPULL_MAX_NETWORK_FAILURES", DEFAULT_MAX_NETWORK_FAILURES),
                        help="open the endpoint circuit after this many concurrent network failures")
    parser.add_argument("--seed", type=str, default=os.environ.get("RAILPULL_SEED", ""),
                        help="optional JSON file with additional train numbers")
    parser.add_argument("--retry-errors", action="store_true",
                        default=os.environ.get("RAILPULL_RETRY_ERRORS", "").lower() in ("1", "true", "yes"),
                        help="retry semantic errors already recorded in errors.json")
    parser.add_argument("--limit", type=int, default=_env_int("RAILPULL_LIMIT", 0),
                        help="process at most N schedules this run (useful for validation)")
    parser.add_argument("--stage", choices=("all", "discovery", "schedules"),
                        default=os.environ.get("RAILPULL_STAGE", "all"))
    args = parser.parse_args(argv)
    args.pause = max(MIN_PAUSE, args.pause)
    args.jitter = max(0.0, args.jitter)
    args.workers = max(1, args.workers)
    args.connect_timeout = max(1.0, args.connect_timeout)
    args.read_timeout = max(1.0, args.read_timeout)
    args.attempts = max(1, args.attempts)
    args.checkpoint_every = max(1, args.checkpoint_every)
    args.checkpoint_seconds = max(1.0, args.checkpoint_seconds)
    args.heartbeat_seconds = max(2.0, args.heartbeat_seconds)
    args.max_network_failures = max(1, args.max_network_failures)
    args.limit = max(0, args.limit)
    return args


def _handle_signal(signum, _frame):
    if not _stop_event.is_set():
        log(f"received signal {signum}; finishing current checkpoint and stopping")
        _stop_event.set()


def main(argv=None):
    global _status_store
    args = parse_args(argv)
    config = Config(
        pause=args.pause, jitter=args.jitter, workers=args.workers,
        connect_timeout=args.connect_timeout, read_timeout=args.read_timeout,
        attempts=args.attempts, checkpoint_every=args.checkpoint_every,
        checkpoint_seconds=args.checkpoint_seconds, heartbeat_seconds=args.heartbeat_seconds,
        max_network_failures=args.max_network_failures, retry_errors=args.retry_errors,
        limit=args.limit,
    )
    _stop_event.clear()
    _status_store = StatusStore(STATUS)
    heartbeat = Heartbeat(_status_store, config.heartbeat_seconds)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, _handle_signal)

    SCHED.mkdir(parents=True, exist_ok=True)
    try:
        seed_numbers = read_seed(args.seed) if args.seed else []
        if seed_numbers:
            log(f"seeded {len(seed_numbers)} extra numbers")
        with crawler_lock():
            _status_store.update(
                phase="starting", stage=args.stage, pid=os.getpid(),
                runId=str(uuid.uuid4()), runStartedAt=iso_now(), startedAt=iso_now(),
                workers=config.workers, pauseSeconds=config.pause,
                heartbeatAt=iso_now(), currentTask="initializing",
            )
            heartbeat.start()
            discover.limiter = RateLimiter(config.pause, config.jitter)
            discover.circuit = EndpointCircuit(config.max_network_failures)
            if args.stage == "schedules":
                raw = load_json(NUMBERS, {})
                roster = {}
                if isinstance(raw, dict):
                    for key, value in raw.items():
                        if str(key).startswith("__"):
                            continue
                        number = normalize_train_number(key)
                        if number:
                            roster[number] = value
                if not roster:
                    raise ValueError(f"no discovered roster at {NUMBERS}; run discovery first")
            else:
                client = make_client(config)
                roster = discover(client, seed_numbers, config)
            if args.stage != "discovery":
                return fetch_schedules(roster, config)
            status(phase="DONE", stage="discovery", currentTask="discovery complete", lastCheckpointAt=iso_now())
            return 0
    except LockUnavailable as exc:
        log(str(exc))
        return 1
    except RuntimeError as exc:
        log(str(exc))
        status(phase="FAILED", pid=os.getpid(), lastError=str(exc), currentTask="stopped")
        return 1
    except StalledWork as exc:
        log(f"crawler stalled: {exc}")
        status(phase="STALLED", pid=os.getpid(), lastError=str(exc), currentTask="endpoint unavailable")
        return EXIT_STALLED
    except RetryableWork as exc:
        log(f"crawler will resume: {exc}")
        if not _stop_event.is_set():
            status(phase="RETRY_WAIT", pid=os.getpid(), lastError=str(exc))
        return EXIT_RETRYABLE
    except KeyboardInterrupt:
        _stop_event.set()
        status(phase="STOPPED", pid=os.getpid(), currentTask="shutdown requested")
        return 130
    except Exception as exc:
        log(f"crawler failed: {type(exc).__name__}: {exc}")
        status(phase="FAILED", pid=os.getpid(), lastError=f"{type(exc).__name__}: {exc}")
        return 1
    finally:
        if heartbeat.is_alive():
            heartbeat.stop()
            heartbeat.join(timeout=5)


if __name__ == "__main__":
    sys.exit(main())
