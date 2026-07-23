import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

from producer import state_to_dict, FIELDS


def make_state(**overrides):
    """Build a fake OpenSky state-vector array, in the correct field order,
    with sensible defaults, overridden by any kwargs passed in."""
    defaults = {
        "icao24": "abc123",
        "callsign": "BAW123  ",
        "origin_country": "United Kingdom",
        "time_position": 1700000000,
        "last_contact": 1700000000,
        "longitude": -0.4543,
        "latitude": 51.4700,
        "baro_altitude": 10000.0,
        "on_ground": False,
        "velocity": 230.0,
        "true_track": 180.0,
        "vertical_rate": 0.0,
        "sensors": None,
        "geo_altitude": 10100.0,
        "squawk": "1000",
        "spi": False,
        "position_source": 0,
    }
    defaults.update(overrides)
    return [defaults[field] for field in FIELDS]


def test_state_to_dict_maps_all_expected_fields():
    state = make_state()
    record = state_to_dict(state)

    assert record["icao24"] == "abc123"
    assert record["origin_country"] == "United Kingdom"
    assert record["longitude"] == -0.4543
    assert record["latitude"] == 51.4700
    assert record["baro_altitude_m"] == 10000.0
    assert record["velocity_ms"] == 230.0
    assert record["heading_deg"] == 180.0
    assert record["vertical_rate"] == 0.0
    assert record["on_ground"] is False
    assert record["time_position"] == 1700000000


def test_state_to_dict_strips_whitespace_from_callsign():
    state = make_state(callsign="BAW123  ")
    record = state_to_dict(state)
    assert record["callsign"] == "BAW123"


def test_state_to_dict_converts_blank_callsign_to_none():
    state = make_state(callsign="        ")
    record = state_to_dict(state)
    assert record["callsign"] is None


def test_state_to_dict_handles_missing_callsign_field():
    state = make_state(callsign=None)
    record = state_to_dict(state)
    assert record["callsign"] is None


def test_state_to_dict_preserves_null_coordinates():
    state = make_state(latitude=None, longitude=None)
    record = state_to_dict(state)
    assert record["latitude"] is None
    assert record["longitude"] is None
