"""Regression tests for tolerant _parse_utc_iso / non-mangling _to_utc_iso.

Covers the failure class observed on a production JetStream fleet: servers and
clients emit RFC 3339 tails nats-py's parser could not read back, blinding any
consumer of the typed jsm() API (ConsumerConfig/ConsumerInfo/PeerInfo).

Run: pytest tests/test_tolerant_utc_iso.py -q
"""

import datetime

from nats.js.api import Base, ConsumerConfig

FRACTIONLESS_Z = "2026-08-28T05:27:00Z"
FRACTION_NO_TZ = "2026-08-28T05:27:00.123"
FRACTION_NEG_OFFSET = "2026-08-28T05:27:00.123-05:00"
NANOSECONDS = "2026-08-28T05:27:00.123456789+00:00"
POS_OFFSET = "2026-08-28T05:27:00+02:00"


def test_parse_fractionless_z():
    dt = Base._parse_utc_iso(FRACTIONLESS_Z)
    assert dt == datetime.datetime(2026, 8, 28, 5, 27, tzinfo=datetime.timezone.utc)


def test_parse_fraction_no_timezone_assumes_utc():
    dt = Base._parse_utc_iso(FRACTION_NO_TZ)
    assert dt == datetime.datetime(2026, 8, 28, 5, 27, 0, 123000, tzinfo=datetime.timezone.utc)


def test_parse_fraction_negative_offset():
    dt = Base._parse_utc_iso(FRACTION_NEG_OFFSET)
    assert dt == datetime.datetime(2026, 8, 28, 10, 27, 0, 123000, tzinfo=datetime.timezone.utc)


def test_parse_nanoseconds_truncated_to_micro():
    dt = Base._parse_utc_iso(NANOSECONDS)
    assert dt == datetime.datetime(2026, 8, 28, 5, 27, 0, 123456, tzinfo=datetime.timezone.utc)


def test_parse_positive_offset_no_fraction():
    dt = Base._parse_utc_iso(POS_OFFSET)
    assert dt == datetime.datetime(2026, 8, 28, 3, 27, tzinfo=datetime.timezone.utc)


def test_consumer_config_from_response_fractionless_opt_start_time():
    """The production failure: opt_start_time without fractional seconds."""
    cfg = ConsumerConfig.from_response({"opt_start_time": FRACTIONLESS_Z})
    assert cfg.opt_start_time == datetime.datetime(2026, 8, 28, 5, 27, tzinfo=datetime.timezone.utc)


def test_consumer_config_from_response_all_shapes():
    for ts in (FRACTIONLESS_Z, FRACTION_NO_TZ, FRACTION_NEG_OFFSET, NANOSECONDS, POS_OFFSET):
        cfg = ConsumerConfig.from_response({"opt_start_time": ts})
        assert cfg.opt_start_time is not None
        assert cfg.opt_start_time.tzinfo is datetime.timezone.utc


def test_cluster_info_leader_since_still_parsed():
    from nats.js.api import ClusterInfo

    info = ClusterInfo.from_response({"leader": "peer-a", "leader_since": FRACTIONLESS_Z})
    assert info.leader_since == datetime.datetime(2026, 8, 28, 5, 27, tzinfo=datetime.timezone.utc)


def test_to_utc_iso_no_longer_strips_zero_fraction():
    """Writer must not mint timestamps its own parser treats differently."""
    dt = datetime.datetime(2026, 8, 28, 5, 27, 0, tzinfo=datetime.timezone.utc)
    out = Base._to_utc_iso(dt)
    assert out == "2026-08-28T05:27:00Z" or out == "2026-08-28T05:27:00+00:00"
    # Whatever it emits, it must parse back to the same instant.
    assert Base._parse_utc_iso(out) == dt


def test_round_trip_all_shapes():
    for ts in (FRACTIONLESS_Z, FRACTION_NO_TZ, FRACTION_NEG_OFFSET, NANOSECONDS, POS_OFFSET):
        dt = Base._parse_utc_iso(ts)
        out = Base._to_utc_iso(dt)
        assert Base._parse_utc_iso(out) == dt
