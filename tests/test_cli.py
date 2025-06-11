from typer.testing import CliRunner
from message_pack_parser.main import app

runner = CliRunner()

def test_cli_invalid_stat_fails():
    """Test that the CLI exits with an error code for an invalid stat name."""
    result = runner.invoke(
        app,
        [
            "run", "replay123", 
            "-i", "./dummy", "-c", "./dummy", "-o", "./dummy", 
            "--compute-stat", "this_is_an_invalid_stat"
        ]
    )
    assert result.exit_code != 0
    assert "Invalid stat" in result.stdout
    assert "this_is_an_invalid_stat" in result.stdout