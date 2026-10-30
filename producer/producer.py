"""Replay a StatsBomb match into Kafka as a simulated live event feed.

Each StatsBomb event is wrapped in an envelope with match metadata and published to Kafka
(key = match_id, so a match's events stay ordered within one partition). Sleeps between
events are scaled by --speed to mimic a live feed, and a small fraction of events can be
held back and sent late to exercise Spark's watermarking.

Run command:
    uv run python -m producer.producer --speed 30
"""

from __future__ import annotations

import argparse
import heapq
import json
import logging
import os
import random
import signal
import time
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

log = logging.getLogger("producer")

# Match-clock offset (seconds) at which each period starts. StatsBomb timestamps restart at
# 00:00 every period; fixed offsets with gaps keep stoppage time from overlapping the next
# period. Must stay in sync with the Spark job.
PERIOD_OFFSETS_S: dict[int, int] = {1: 0, 2: 60 * 60, 3: 120 * 60, 4: 150 * 60, 5: 180 * 60}


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested)
# ---------------------------------------------------------------------------
def parse_timestamp(ts: str) -> float:
    """'HH:MM:SS.fff' -> seconds."""
    hours, minutes, seconds = ts.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def match_clock_seconds(event: dict[str, Any]) -> float:
    """Seconds since kickoff on the continuous match clock (period offset + in-period time)."""
    return PERIOD_OFFSETS_S[event["period"]] + parse_timestamp(event["timestamp"])


def kickoff_iso(match_date: str, kick_off: str | None) -> str:
    """Build an ISO-8601 UTC kickoff timestamp from StatsBomb's match_date + kick_off."""
    time_part = (kick_off or "00:00:00.000")[:8]
    return f"{match_date}T{time_part}Z"


def build_envelope(event: dict[str, Any], meta: MatchMeta, delay_s: float = 0.0) -> dict[str, Any]:
    """Wrap a raw (nested) StatsBomb event with match metadata."""
    return {
        "match_id": meta.match_id,
        "home_team": meta.home_team,
        "away_team": meta.away_team,
        "kickoff_ts": meta.kickoff_ts,
        "produced_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "simulated_delay_s": round(delay_s, 3),
        "event": event,
    }


def sleep_seconds(prev_clock: float, clock: float, speed: float, max_sleep: float) -> float:
    """Real seconds to wait between two events. speed <= 0 disables sleeping."""
    if speed <= 0:
        return 0.0
    return min(max(clock - prev_clock, 0.0) / speed, max_sleep)


# ---------------------------------------------------------------------------
# StatsBomb loading
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class MatchMeta:
    match_id: int
    home_team: str
    away_team: str
    kickoff_ts: str


def load_match(competition_id: int, season_id: int, match_id: int):
    """Return (MatchMeta, events sorted by match clock) from StatsBomb open data."""
    from statsbombpy import sb  # imported lazily: slow import, not needed by unit tests

    warnings.filterwarnings("ignore", message=".*credentials were not supplied.*")

    matches = sb.matches(competition_id=competition_id, season_id=season_id)
    rows = matches[matches.match_id == match_id]
    if rows.empty:
        raise SystemExit(
            f"match_id={match_id} not found in competition={competition_id} season={season_id}"
        )
    row = rows.iloc[0]
    meta = MatchMeta(
        match_id=int(row.match_id),
        home_team=str(row.home_team),
        away_team=str(row.away_team),
        kickoff_ts=kickoff_iso(str(row.match_date), row.kick_off),
    )
    events = sorted(
        sb.events(match_id=match_id, fmt="dict").values(),
        key=lambda e: (match_clock_seconds(e), e["index"]),
    )
    return meta, events


def list_matches(competition_id: int, season_id: int) -> None:
    from statsbombpy import sb

    warnings.filterwarnings("ignore", message=".*credentials were not supplied.*")
    matches = sb.matches(competition_id=competition_id, season_id=season_id)
    cols = ["match_id", "match_date", "home_team", "away_team", "home_score", "away_score"]
    print(matches[cols].sort_values("match_date").to_string(index=False))


# ---------------------------------------------------------------------------
# Kafka publishing
# ---------------------------------------------------------------------------
@dataclass
class Stats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    late: int = 0
    max_delay_s: float = 0.0
    errors: list[str] = field(default_factory=list)


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "momentum-producer",
            "acks": "all",
            "enable.idempotence": True,  # no duplicates from producer retries
            "linger.ms": 5,
        }
    )


def replay(
    producer: Producer,
    topic: str,
    meta: MatchMeta,
    events: list[dict[str, Any]],
    *,
    speed: float,
    max_sleep: float,
    late_prob: float,
    late_max_s: float,
    max_events: int | None,
    rng: random.Random,
) -> Stats:
    stats = Stats()
    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True
        log.warning("Interrupt received; flushing and exiting...")

    signal.signal(signal.SIGINT, _stop)

    def _on_delivery(err: KafkaError | None, _msg: Message) -> None:
        if err is None:
            stats.delivered += 1
        else:
            stats.failed += 1
            if len(stats.errors) < 5:
                stats.errors.append(str(err))

    key = str(meta.match_id).encode()

    def _send(event: dict[str, Any], delay_s: float = 0.0) -> None:
        payload = json.dumps(build_envelope(event, meta, delay_s), default=str).encode()
        while True:
            try:
                producer.produce(topic, key=key, value=payload, on_delivery=_on_delivery)
                break
            except BufferError:  # local queue full: let it drain
                producer.poll(0.5)
        stats.sent += 1
        producer.poll(0)

    # Held-back "late" events: (release_at_match_clock, seq, clock, event)
    held: list[tuple[float, int, float, dict[str, Any]]] = []

    def _release(until_clock: float, *, force: bool = False) -> None:
        """Send held events due by `until_clock` (or all of them if force=True)."""
        while held and (force or held[0][0] <= until_clock):
            _, _, ev_clock, ev = heapq.heappop(held)
            delay = max(until_clock - ev_clock, 0.0)
            _send(ev, delay)
            stats.late += 1
            stats.max_delay_s = max(stats.max_delay_s, delay)

    selected = events[:max_events] if max_events else events
    prev_clock = match_clock_seconds(selected[0]) if selected else 0.0
    prev_period = selected[0]["period"] if selected else None
    last_log = time.monotonic()

    for seq, event in enumerate(selected):
        if stopping:
            break
        clock = match_clock_seconds(event)

        # Period ended: held events arrive before the next period starts, so the gap between
        # period offsets (e.g. half-time) is never counted as delay.
        if event["period"] != prev_period:
            _release(prev_clock, force=True)
            prev_period = event["period"]

        time.sleep(sleep_seconds(prev_clock, clock, speed, max_sleep))
        prev_clock = clock

        # Release held events whose simulated delay has elapsed on the match clock
        _release(clock)

        if late_prob > 0 and rng.random() < late_prob:
            heapq.heappush(held, (clock + rng.uniform(1, late_max_s), seq, clock, event))
        else:
            _send(event)

        if time.monotonic() - last_log >= 5:
            minute, second = divmod(int(clock), 60)
            log.info(
                "match clock %03d:%02d | sent=%d late=%d held=%d",
                minute,
                second,
                stats.sent,
                stats.late,
                len(held),
            )
            last_log = time.monotonic()

    # Flush anything still held back at the end of the match
    _release(prev_clock, force=True)

    remaining = producer.flush(30)
    if remaining:
        log.error("%d messages were not delivered before timeout", remaining)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(description="Replay a StatsBomb match into Kafka.")
    p.add_argument("--bootstrap", default=env("PRODUCER_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--topic", default=env("KAFKA_TOPIC", "live-match-events"))
    p.add_argument("--competition-id", type=int, default=int(env("COMPETITION_ID", "43")))
    p.add_argument("--season-id", type=int, default=int(env("SEASON_ID", "106")))
    p.add_argument("--match-id", type=int, default=int(env("MATCH_ID", "3869685")))
    p.add_argument(
        "--speed",
        type=float,
        default=float(env("SPEED", "30")),
        help="match seconds per real second (0 = no sleeping)",
    )
    p.add_argument(
        "--max-sleep",
        type=float,
        default=float(env("MAX_SLEEP", "3")),
        help="cap on a single real-time sleep (skips half-time breaks)",
    )
    p.add_argument("--late-prob", type=float, default=float(env("LATE_PROB", "0.03")))
    p.add_argument("--late-max-seconds", type=float, default=float(env("LATE_MAX_SECONDS", "180")))
    p.add_argument("--max-events", type=int, default=None, help="only send the first N events")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducible late events")
    p.add_argument("--list-matches", action="store_true", help="list matches and exit")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    if args.list_matches:
        list_matches(args.competition_id, args.season_id)
        return

    log.info("Loading match %s from StatsBomb open data...", args.match_id)
    meta, events = load_match(args.competition_id, args.season_id, args.match_id)
    log.info(
        "%s vs %s | kickoff %s | %d events -> topic '%s' @ %s (speed=%sx)",
        meta.home_team,
        meta.away_team,
        meta.kickoff_ts,
        len(events),
        args.topic,
        args.bootstrap,
        args.speed,
    )

    stats = replay(
        make_producer(args.bootstrap),
        args.topic,
        meta,
        events,
        speed=args.speed,
        max_sleep=args.max_sleep,
        late_prob=args.late_prob,
        late_max_s=args.late_max_seconds,
        max_events=args.max_events,
        rng=random.Random(args.seed),
    )
    log.info(
        "Done: sent=%d delivered=%d failed=%d late=%d (max delay %.0fs)",
        stats.sent,
        stats.delivered,
        stats.failed,
        stats.late,
        stats.max_delay_s,
    )
    if stats.failed:
        log.error("Delivery errors (first %d): %s", len(stats.errors), stats.errors)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
