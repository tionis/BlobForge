from unittest.mock import patch

from blobforge.conversion_child import main
from blobforge.conversion_runtime import (
    CONVERSION_CONFIGURATION_EXIT_CODE,
    WorkerConfigurationError,
)


def test_child_reports_configuration_failure_with_distinct_exit_code(tmp_path):
    with patch(
        "blobforge.conversion_child.run_conversion",
        side_effect=WorkerConfigurationError("missing Marker"),
    ):
        result = main([str(tmp_path / "source.pdf"), str(tmp_path / "output")])

    assert result == CONVERSION_CONFIGURATION_EXIT_CODE
