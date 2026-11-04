import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))

from consumer import is_valid


def make_record(**overrides):
    defaults = {
        "icao24": "abc123",
        "callsign": "BAW123",
        "origin_country": "United Kingdom",
        "longitude": -0.4543,
        "latitude": 51.4700,
        "baro_altitude_m": 10000.0,
        "velocity_ms": 230.0,
        "heading_deg": 180.0,
        "vertical_rate": 0.0,
        "on_ground": False,
        "time_position": 1700000000,
    }
    defaults.update(overrides)
    return defaults


def test_valid_record_passes():
    assert is_valid(make_record()) is True


def test_rejects_non_dict_input():
    assert is_valid(["not", "a", "dict"]) is False
    assert is_valid(None) is False


def test_rejects_missing_icao24():
    record = make_record(icao24=None)
    assert is_valid(record) is False


def test_rejects_missing_latitude():
    record = make_record(latitude=None)
    assert is_valid(record) is False


def test_rejects_missing_longitude():
    record = make_record(longitude=None)
    assert is_valid(record) is False


def test_rejects_out_of_range_latitude():
    assert is_valid(make_record(latitude=95.0)) is False
    assert is_valid(make_record(latitude=-95.0)) is False


def test_rejects_out_of_range_longitude():
    assert is_valid(make_record(longitude=185.0)) is False
    assert is_valid(make_record(longitude=-185.0)) is False


def test_accepts_boundary_coordinates():
    assert is_valid(make_record(latitude=90.0, longitude=180.0)) is True
    assert is_valid(make_record(latitude=-90.0, longitude=-180.0)) is True


def test_allows_null_optional_fields():
    record = make_record(callsign=None, velocity_ms=None, vertical_rate=None)
    assert is_valid(record) is True


def test_normalize_fills_missing_optional_fields():
    from consumer import normalize, COLUMNS

    record = normalize({"icao24": "abc123", "latitude": 51.0, "longitude": 0.0, "extra": 1})
    assert set(record) == set(COLUMNS)
    assert record["callsign"] is None
    assert "extra" not in record
