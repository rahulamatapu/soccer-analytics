import json
import random

import pytest

from producer.producer import (
    MatchMeta,
    build_envelope,
    kickoff_iso,
    match_clock_seconds,
    parse_timestamp,
    replay,
    sleep_seconds,
)

META = MatchMeta(match_id=1, home_team="Home", away_team="Away", kickoff_ts="2022-12-18T17:00:00Z")


def ev(index: int, period: int, ts: str) -> dict:
    return {"index": index, "period": period, "timestamp": ts, "type": {"name": "Pass"}}


class FakeProducer:
    """Minimal stand-in for confluent_kafka.Producer that records messages."""

    def __init__(self):
        self.messages: list[dict] = []
        self._callbacks = []

    def produce(self, topic, key, value, on_delivery):
        self.messages.append({"topic": topic, "key": key, "value": json.loads(value)})
        self._callbacks.append(on_delivery)

    def poll(self, _timeout=0):
        while self._callbacks:
            self._callbacks.pop(0)(None, None)

    def flush(self, _timeout=0):
        self.poll()
        return 0


def test_parse_timestamp():
    assert parse_timestamp("00:00:00.000") == 0
    assert parse_timestamp("00:12:34.500") == pytest.approx(754.5)
    assert parse_timestamp("01:00:01.000") == 3601


@pytest.mark.parametrize(
    ("period", "expected"),
    [(1, 10), (2, 3600 + 10), (3, 7200 + 10), (4, 9000 + 10), (5, 10800 + 10)],
)
def test_match_clock_uses_period_offsets(period, expected):
    assert match_clock_seconds(ev(1, period, "00:00:10.000")) == expected


def test_first_half_stoppage_does_not_overlap_second_half():
    late_first_half = match_clock_seconds(ev(1, 1, "00:52:34.569"))
    second_half_kickoff = match_clock_seconds(ev(2, 2, "00:00:00.000"))
    assert late_first_half < second_half_kickoff


def test_kickoff_iso():
    assert kickoff_iso("2022-12-18", "17:00:00.000") == "2022-12-18T17:00:00Z"
    assert kickoff_iso("2022-12-18", None) == "2022-12-18T00:00:00Z"


def test_build_envelope_keeps_nested_event():
    event = {"id": "x", "pass": {"end_location": [100.0, 40.0]}}
    env = build_envelope(event, META, delay_s=12.3456)
    assert env["match_id"] == 1
    assert env["home_team"] == "Home" and env["away_team"] == "Away"
    assert env["kickoff_ts"] == "2022-12-18T17:00:00Z"
    assert env["simulated_delay_s"] == 12.346
    assert env["event"] is event


def test_sleep_seconds():
    assert sleep_seconds(0, 60, speed=30, max_sleep=10) == 2
    assert sleep_seconds(0, 3600, speed=30, max_sleep=3) == 3  # capped (half-time)
    assert sleep_seconds(10, 5, speed=30, max_sleep=3) == 0  # never negative
    assert sleep_seconds(0, 60, speed=0, max_sleep=3) == 0  # speed 0 => no sleep


def _replay(events, **kw):
    fake = FakeProducer()
    opts = dict(speed=0, max_sleep=0, late_prob=0, late_max_s=0, max_events=None)
    opts.update(kw)
    stats = replay(fake, "t", META, events, rng=random.Random(0), **opts)
    return fake, stats


def test_replay_sends_all_events_in_order_keyed_by_match():
    events = [ev(i, 1, f"00:00:{i:02d}.000") for i in range(10)]
    fake, stats = _replay(events)
    assert stats.sent == stats.delivered == 10 and stats.late == 0
    assert [m["value"]["event"]["index"] for m in fake.messages] == list(range(10))
    assert {m["key"] for m in fake.messages} == {b"1"}


def test_replay_late_events_are_all_eventually_sent():
    events = [ev(i, 1, f"00:{i // 60:02d}:{i % 60:02d}.000") for i in range(300)]
    fake, stats = _replay(events, late_prob=0.2, late_max_s=60)
    assert stats.sent == 300 and stats.late > 0
    indexes = [m["value"]["event"]["index"] for m in fake.messages]
    assert sorted(indexes) == list(range(300))  # nothing lost or duplicated
    assert indexes != list(range(300))  # but some arrived out of order
    delayed = [m for m in fake.messages if m["value"]["simulated_delay_s"] > 0]
    assert len(delayed) == stats.late


def test_replay_max_events():
    events = [ev(i, 1, f"00:00:{i:02d}.000") for i in range(10)]
    _, stats = _replay(events, max_events=3)
    assert stats.sent == 3


def test_late_events_never_wait_across_a_period_break():
    # 1st half ends at 45:00; 2nd half starts at match clock 60:00 (15-min offset gap)
    first = [ev(i, 1, f"00:{40 + i // 60:02d}:{i % 60:02d}.000") for i in range(300)]
    second = [ev(300 + i, 2, f"00:00:{i:02d}.000") for i in range(30)]
    fake, stats = _replay(first + second, late_prob=0.3, late_max_s=120)
    assert stats.late > 0
    assert stats.max_delay_s <= 121  # late_max_s + 1s spacing between events
    # every 1st-half event is delivered before the first 2nd-half event
    periods = [m["value"]["event"]["period"] for m in fake.messages]
    assert periods == sorted(periods)
