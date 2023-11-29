from __future__ import annotations

from enum import Enum
from typing import Dict


class RunnerMode(str, Enum):
    RUNNER_MODE_UNSET = "RUNNER_MODE_UNSET"
    RUNNER_MODE_CLOUD = "RUNNER_MODE_CLOUD"
    RUNNER_MODE_HYBRID = "RUNNER_MODE_HYBRID"

    @staticmethod
    def from_str(
        unstrict: str,
        _string_map: Dict[str, RunnerMode] = {
            "RUNNER_MODE_UNSET": RUNNER_MODE_UNSET,
            "unset": RUNNER_MODE_UNSET,
            "RUNNER_MODE_CLOUD": RUNNER_MODE_CLOUD,
            "cloud": RUNNER_MODE_CLOUD,
            "RUNNER_MODE_HYBRID": RUNNER_MODE_HYBRID,
            "hybrid": RUNNER_MODE_HYBRID,
        },
    ) -> RunnerMode:
        if unstrict not in _string_map:
            raise ValueError(
                f"Workflows only supported for 'cloud' or 'hybrid', not '{unstrict}'"
            )
        return _string_map[unstrict]
