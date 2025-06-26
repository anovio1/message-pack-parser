import pytest
import polars as pl
from polars.testing import assert_frame_equal

from tubuin_processor.core.aggregator import (
    _calculate_damage_by_unit_def,
    _calculate_resources_by_team,
    _calculate_aggression_by_unit,
    perform_aggregations,
    STATS_REGISTRY
)


@pytest.fixture
def sample_damage_log() -> pl.DataFrame:
    """Provides a sample damage_log DataFrame for testing."""
    return pl.DataFrame({
        "attacker_def_id": [101, 102, 101, None, 102],
        "damage": [100, 50, 200, 999, 150]
    })

def test_calculate_damage_by_unit_def(sample_damage_log):
    result = _calculate_damage_by_unit_def({"damage_log": sample_damage_log})
    expected = pl.DataFrame({
        "unit_def_id": [101, 102],
        "total_damage_dealt": [300.0, 200.0]
    }).sort("total_damage_dealt", descending=True)
    assert_frame_equal(result, expected.select(pl.all().cast(result.schema)))

def test_perform_aggregations_computes_all_by_default(mocker):
    """Test that if no stats are requested, all stats in the registry are computed."""
    # Mock the helper functions to isolate the orchestration logic
    mock_calc_damage = mocker.patch(
        'tubuin_processor.core.aggregator._calculate_damage_by_unit_def', 
        return_value=pl.DataFrame({"unit_def_id": [1]})
    )
    mock_calc_resources = mocker.patch(
        'tubuin_processor.core.aggregator._calculate_resources_by_team',
        return_value=pl.DataFrame({"team_id": [1]})
    )
    
    agg_stats, _ = perform_aggregations(dataframes_by_aspect={}, stats_to_compute=[])
    
    mock_calc_damage.assert_called_once()
    mock_calc_resources.assert_called_once()
    assert "damage_by_unit_def" in agg_stats
    assert "resources_by_team" in agg_stats
    
@pytest.fixture
def sample_dataframes() -> dict:
    """Provides a dictionary of sample DataFrames for testing aggregations."""
    return {
        "damage_log": pl.DataFrame({
            "attacker_def_id": [101, 102, 101, None, 102],
            "damage": [100.0, 50.0, 200.0, 999.0, 150.0] # Damage is now float after transformation
        }),
        "unit_positions": pl.DataFrame({
            "unit_id": [1, 1, 2, 2],
            "frame": [0, 30, 0, 60], # Unit 1 moves for 1s, Unit 2 moves for 2s
            "x": [10, 20, 100, 100],
            "y": [10, 10, 100, 130] # Unit 2 moves 30 units vertically
        })
    }

def test_calculate_aggression_by_unit(sample_dataframes):
    result = _calculate_aggression_by_unit(sample_dataframes)
    
    # Unit 2 moves further and should have a higher score
    assert result.sort("unit_id")["aggression_score"][0] < result.sort("unit_id")["aggression_score"][1]
    assert result.columns == ["unit_id", "aggression_score"]
    assert result.dtypes == [pl.Int64, pl.Float64]

def test_perform_aggregations_computes_defaults(mocker):
    """Test that if no stats are requested, default stats are computed."""
    mock_calc_damage = mocker.patch('tubuin_processor.core.aggregator._calculate_damage_by_unit_def', return_value=pl.DataFrame())
    mock_calc_resources = mocker.patch('tubuin_processor.core.aggregator._calculate_resources_by_team', return_value=pl.DataFrame())
    mock_calc_aggression = mocker.patch('tubuin_processor.core.aggregator._calculate_aggression_by_unit', return_value=pl.DataFrame())

    perform_aggregations(dataframes_by_aspect={}, stats_to_compute=[])
    
    # Assert that the two stats marked as default=True were called
    mock_calc_damage.assert_called_once()
    mock_calc_resources.assert_called_once()
    # Assert that the non-default stat was NOT called
    mock_calc_aggression.assert_not_called()