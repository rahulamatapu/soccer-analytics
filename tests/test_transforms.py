"""Unit tests for pure Spark transformation functions."""

import json
from datetime import datetime

import pytest

from spark.momentum.config import Settings
from spark.momentum.transforms import (
    add_event_time,
    calculate_momentum_window,
    extract_features,
    parse_kafka_stream,
)


def make_raw_envelope(
    match_id: int = 100,
    home_team: str = "Argentina",
    away_team: str = "France",
    kickoff_ts: str = "2022-12-18T17:00:00Z",
    period: int = 1,
    timestamp: str = "00:02:30.000",
    event_type: str = "Pass",
    team_name: str = "Argentina",
    location: list[float] | None = None,
    pass_end_location: list[float] | None = None,
    pass_outcome: str | None = None,
    shot_xg: float | None = None,
    shot_outcome: str | None = None,
    duel_type: str | None = None,
) -> str:
    """Helper to build a raw JSON string matching the Kafka message value."""
    event_body = {
        "id": "ev-01",
        "index": 1,
        "period": period,
        "timestamp": timestamp,
        "minute": int(timestamp.split(":")[1]),
        "second": int(float(timestamp.split(":")[2])),
        "type": {"name": event_type},
        "team": {"name": team_name},
        "location": location or [50.0, 40.0],
    }
    if event_type == "Pass":
        event_body["pass"] = {
            "end_location": pass_end_location or [60.0, 40.0],
            "outcome": {"name": pass_outcome} if pass_outcome else None,
        }
    elif event_type == "Shot":
        event_body["shot"] = {
            "statsbomb_xg": shot_xg,
            "outcome": {"name": shot_outcome or "Saved"},
        }
    elif event_type == "Duel":
        event_body["duel"] = {"type": {"name": duel_type or "Tackle"}}

    payload = {
        "match_id": match_id,
        "home_team": home_team,
        "away_team": away_team,
        "kickoff_ts": kickoff_ts,
        "produced_at": "2022-12-18T17:02:30Z",
        "simulated_delay_s": 0.0,
        "event": event_body,
    }
    return json.dumps(payload)


def test_parse_kafka_stream(spark):
    valid_json = make_raw_envelope(
        event_type="Shot",
        shot_xg=0.25,
        location=[95.0, 35.0],
    )
    malformed_json = "NOT_VALID_JSON"

    raw_df = spark.createDataFrame(
        [(valid_json.encode("utf-8"),), (malformed_json.encode("utf-8"),)],
        ["value"],
    )

    parsed = parse_kafka_stream(raw_df).collect()
    assert len(parsed) == 1

    row = parsed[0]
    assert row.match_id == 100
    assert row.home_team == "Argentina"
    assert row.event_type == "Shot"
    assert row.shot_xg == pytest.approx(0.25)
    assert row.location == [95.0, 35.0]


def test_add_event_time(spark):
    events = [
        make_raw_envelope(period=1, timestamp="00:10:00.000"),  # 10 min into period 1
        make_raw_envelope(
            period=2, timestamp="00:05:00.000"
        ),  # 5 min into period 2 (offset = 60 min)
    ]
    raw_df = spark.createDataFrame([(e.encode("utf-8"),) for e in events], ["value"])
    parsed = parse_kafka_stream(raw_df)
    with_time = add_event_time(parsed).sort("period").collect()

    # Kickoff: 2022-12-18 17:00:00
    # Period 1, 10m -> 17:10:00
    assert with_time[0].event_time == datetime(2022, 12, 18, 17, 10, 0)
    # Period 2, 5m -> 17:00:00 + 60m + 5m = 18:05:00
    assert with_time[1].event_time == datetime(2022, 12, 18, 18, 5, 0)


def test_extract_features(spark):
    events = [
        # 0: Completed pass inside own half
        make_raw_envelope(
            event_type="Pass",
            pass_outcome=None,
            pass_end_location=[50.0, 30.0],
            team_name="Argentina",
        ),
        # 1: Completed final-third pass (X >= 80)
        make_raw_envelope(
            event_type="Pass",
            pass_outcome=None,
            pass_end_location=[85.0, 30.0],
            team_name="Argentina",
        ),
        # 2: Incomplete pass (even if in final third, not completed)
        make_raw_envelope(
            event_type="Pass",
            pass_outcome="Incomplete",
            pass_end_location=[90.0, 30.0],
            team_name="Argentina",
        ),
        # 3: Away shot with xG
        make_raw_envelope(event_type="Shot", shot_xg=0.42, team_name="France"),
        # 4: Away tackle
        make_raw_envelope(event_type="Duel", duel_type="Tackle", team_name="France"),
        # 5: Aerial duel (not a tackle)
        make_raw_envelope(event_type="Duel", duel_type="Aerial Lost", team_name="France"),
    ]
    raw_df = spark.createDataFrame([(e.encode("utf-8"),) for e in events], ["value"])
    features = extract_features(parse_kafka_stream(raw_df)).collect()

    # Pass in own half
    assert features[0].is_completed_pass is True
    assert features[0].is_final_third_pass is False
    assert features[0].is_home is True

    # Final-third pass
    assert features[1].is_completed_pass is True
    assert features[1].is_final_third_pass is True

    # Incomplete pass
    assert features[2].is_completed_pass is False
    assert features[2].is_final_third_pass is False

    # Shot & xG
    assert features[3].is_shot is True
    assert features[3].xg == pytest.approx(0.42)
    assert features[3].is_away is True

    # Tackle
    assert features[4].is_tackle is True

    # Aerial duel
    assert features[5].is_tackle is False


def test_calculate_momentum_window(spark):
    settings = Settings.from_env()

    # Create events clustered within the same 5-minute window (minute 01:00 to 03:00)
    events = [
        # Argentina (Home): 3 completed passes (1 final-third), 1 shot (xG 0.4), 1 tackle
        make_raw_envelope(
            timestamp="00:01:00.000",
            event_type="Pass",
            pass_end_location=[50.0, 20.0],
            team_name="Argentina",
        ),
        make_raw_envelope(
            timestamp="00:01:30.000",
            event_type="Pass",
            pass_end_location=[60.0, 20.0],
            team_name="Argentina",
        ),
        make_raw_envelope(
            timestamp="00:02:00.000",
            event_type="Pass",
            pass_end_location=[88.0, 20.0],
            team_name="Argentina",
        ),
        make_raw_envelope(
            timestamp="00:02:15.000",
            event_type="Shot",
            shot_xg=0.4,
            team_name="Argentina",
        ),
        make_raw_envelope(
            timestamp="00:02:45.000",
            event_type="Duel",
            duel_type="Tackle",
            team_name="Argentina",
        ),
        # France (Away): 1 completed pass, 0 shots, 0 tackles
        make_raw_envelope(
            timestamp="00:03:00.000",
            event_type="Pass",
            pass_end_location=[45.0, 20.0],
            team_name="France",
        ),
    ]

    raw_df = spark.createDataFrame([(e.encode("utf-8"),) for e in events], ["value"])
    processed = extract_features(add_event_time(parse_kafka_stream(raw_df)))

    # In batch test mode, apply_watermark=False allows sliding window
    # aggregation on static DataFrames
    result = calculate_momentum_window(processed, settings, apply_watermark=False).collect()
    assert len(result) > 0

    # Pick the window covering minute 1 to 3 (e.g. window_start: 17:00:00 to 17:05:00)
    window_row = [r for r in result if r.window_start == datetime(2022, 12, 18, 17, 0, 0)][0]

    # Passes & possession: Arg 3, Fra 1 => 4 total => Arg 75%, Fra 25%
    assert window_row.home_completed_passes == 3
    assert window_row.away_completed_passes == 1
    assert window_row.home_possession_pct == pytest.approx(75.0)
    assert window_row.away_possession_pct == pytest.approx(25.0)

    # Shots & xG
    assert window_row.home_shots == 1
    assert window_row.away_shots == 0
    assert window_row.home_total_xg == pytest.approx(0.4)
    assert window_row.home_avg_xg == pytest.approx(0.4)

    # Momentum weights: final_third=1.0, shot=3.0, xg=5.0, tackle=0.5
    # Argentina momentum = (1 * 1.0) + (1 * 3.0) + (0.4 * 5.0) + (1 * 0.5) = 1 + 3 + 2 + 0.5 = 6.5
    # France momentum = 0
    assert window_row.home_momentum == pytest.approx(6.5)
    assert window_row.away_momentum == pytest.approx(0.0)
    assert window_row.momentum_diff == pytest.approx(6.5)


def test_build_pipeline_schema(spark):
    from spark.jobs.momentum_stream import build_pipeline

    settings = Settings.from_env()
    raw_event = make_raw_envelope(event_type="Pass")
    raw_df = spark.createDataFrame([(raw_event.encode("utf-8"),)], ["value"])

    out = build_pipeline(raw_df, settings, apply_watermark=False)

    expected_cols = {
        "match_id",
        "home_team",
        "away_team",
        "window_start",
        "window_end",
        "home_completed_passes",
        "away_completed_passes",
        "home_possession_pct",
        "away_possession_pct",
        "home_shots",
        "away_shots",
        "home_avg_xg",
        "away_avg_xg",
        "home_momentum",
        "away_momentum",
        "momentum_diff",
    }
    assert expected_cols.issubset(set(out.columns))
