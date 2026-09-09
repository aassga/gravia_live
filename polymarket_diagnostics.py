"""Bounded, read-only decision evidence shared by simulation and live paths."""

import hashlib
import json
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count

RUN_ID = str(time.time_ns())
MAX_SAMPLES = 12
MAX_EVIDENCE_WINDOWS = 8
TARGET_VARIANTS = {
    value.strip()
    for value in os.environ.get(
        "POLY_DECISION_EVIDENCE_VARIANTS", "btc-historical-hybrid"
    ).split(",")
    if value.strip()
}
_sequence = count(1)
_context = ContextVar("decision_evidence", default=None)
trigger = ContextVar("decision_trigger", default="market_ws")
IMPORTANT = {"entered", "hedged", "pair_candidate", "direction_candidate",
             "pair_batch_submitted", "pair_batch_result", "pair_batch_filled",
             "entry_order_filled", "entry_order_not_filled", "entry_order_unconfirmed",
             "pair_candidate_vanished_before_submit", "settled"}


def book_snapshot(book, now):
    received = book.get("receivedAtMonotonic")
    levels = {side: [dict(level) for level in book.get(side, [])[:6]]
              for side in ("bids", "asks")}
    identity = {**levels, "receivedAtMonotonic": received,
                "quoteSource": book.get("quoteSource"),
                "tickSize": book.get("tickSize"), "minOrderSize": book.get("minOrderSize")}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
    return {**identity, "fingerprint": fingerprint,
            "ageMs": None if received is None else round((now - received) * 1000, 3),
            "askDepth": sum(float(x.get("size", 0)) for x in book.get("asks", []))}


@contextmanager
def evaluation(stream, variant, slug, source, up, down, remaining, settings, signal, latest_books):
    token = _context.set({"stream": stream, "variant": variant, "windowSlug": slug,
                          "trigger": source, "up": up, "down": down,
                          "remainingSeconds": remaining, "settings": settings,
                          "signal": signal, "latest_books": latest_books,
                          "startedAt": time.time(), "startedMonotonic": time.monotonic(),
                          "evaluationId": f"{RUN_ID}:{next(_sequence)}"})
    try:
        yield
    finally:
        _context.reset(token)


def record(item, reason, details):
    ctx = _context.get()
    if not ctx or ctx["variant"] not in TARGET_VARIANTS:
        return
    try:
        _record(item, reason, details)
    except Exception as exc:
        # Observability must never interrupt an entry, rescue or settlement.
        item["decisionEvidenceError"] = type(exc).__name__


def _record(item, reason, details):
    ctx = _context.get()
    if not ctx or not reason or ctx["windowSlug"] != item.get("windowSlug"):
        return
    samples = item.setdefault("decisionSamples", [])
    now = time.monotonic()
    seen = item.setdefault("sampledReasons", [])
    reason_key = ctx["trigger"] + ":" + reason
    first = reason_key not in seen
    important = reason in IMPORTANT
    sample_times = item.setdefault("decisionSampleTimes", {})
    source_key = ctx["trigger"]
    previous = sample_times.get(source_key)
    if not first and not important and previous and previous[0] == RUN_ID:
        if now - previous[1] < 1.0:
            return
    if first:
        seen.append(reason_key)
    sample_times[source_key] = [RUN_ID, now]
    # Snapshot only sampled decisions. No disk, network, signing or order submission here.
    sample = {k: ctx[k] for k in ("stream", "variant", "windowSlug", "trigger",
              "remainingSeconds", "settings", "signal", "startedAt", "evaluationId")}
    sample.update({"runId": RUN_ID, "reason": reason, "observedAt": time.time(),
                   "sampleMonotonic": now, "evaluationElapsedMs": round((now-ctx["startedMonotonic"])*1000, 3),
                   "details": dict(details), "upBook": book_snapshot(ctx["up"], now),
                   "downBook": book_snapshot(ctx["down"], now)})
    latest_up, latest_down = ctx["latest_books"]()
    sample["latestWsUpBook"] = book_snapshot(latest_up or {}, now)
    sample["latestWsDownBook"] = book_snapshot(latest_down or {}, now)
    sample["matchesLatestWs"] = all(sample[a]["fingerprint"] == sample[b]["fingerprint"]
        for a, b in (("upBook", "latestWsUpBook"), ("downBook", "latestWsDownBook")))
    if sample["matchesLatestWs"]:
        for key in ("latestWsUpBook", "latestWsDownBook"):
            sample[key] = {"fingerprint": sample[key]["fingerprint"]}
    samples.append(sample)
    del samples[:-MAX_SAMPLES]
    if important:
        events = item.setdefault("decisionEvents", [])
        events.append(sample)
        del events[:-8]


def public_summary(item):
    """Full snapshots stay in persisted diagnostics, not every dashboard frame."""
    return {**{k: v for k, v in item.items()
               if k not in ("decisionSamples", "decisionEvents", "sampledReasons", "decisionSampleTimes")},
            "decisionSampleCount": len(item.get("decisionSamples", [])),
            "decisionEventCount": len(item.get("decisionEvents", []))}


def trim_old_window_evidence(items):
    """Keep aggregate outcomes but bound full-book evidence in active state."""
    for item in items[:MAX_EVIDENCE_WINDOWS]:
        samples = item.get("decisionSamples")
        if samples:
            del samples[:-MAX_SAMPLES]
        events = item.get("decisionEvents")
        if events:
            del events[:-8]
    for item in items[MAX_EVIDENCE_WINDOWS:]:
        for key in (
            "decisionSamples", "decisionEvents", "sampledReasons", "decisionSampleTimes"
        ):
            item.pop(key, None)
