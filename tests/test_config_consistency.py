import pytest
from tubuin_processor.utils.config_validator import validate_configurations

def test_all_configurations_are_consistent():
    """
    This test ensures that all schema and dynamic config mappings are in sync.
    It acts as a CI guard against configuration drift.
    """
    try:
        validate_configurations()
    except AssertionError as e:
        pytest.fail(f"Configuration consistency check failed: {e}")