from unittest.mock import MagicMock, PropertyMock

import click
import pytest

from gretel_client.cli.common import _determine_runner_mode, _model_runner_modes
from gretel_client.config import RunnerMode


def test_runner_arg_is_honored():
    # Ensures that an explicitly specified runner mode is always honored (if compatible)
    # and doesn't cause any log statements.
    for runner_arg in RunnerMode:
        project_runner_modes = [None]
        if runner_arg in (RunnerMode.CLOUD, RunnerMode.HYBRID):
            project_runner_modes.append(runner_arg)
        for project_runner_mode in project_runner_modes:
            project = MagicMock(runner_mode=project_runner_mode)

            for session_default in RunnerMode:
                sc = MagicMock(session=MagicMock(default_runner=session_default))

                model_runner_modes = [None, runner_arg]
                if runner_arg in (RunnerMode.MANUAL, RunnerMode.LOCAL):
                    model_runner_modes.extend([RunnerMode.CLOUD])

                for model_runner_mode in model_runner_modes:
                    model = (
                        MagicMock(runner_mode=model_runner_mode)
                        if model_runner_mode
                        else None
                    )

                    model_jsons = [None]
                    if model_runner_mode:
                        model_jsons.append({"runner_mode": model_runner_mode})

                    for model_json in model_jsons:

                        actual_runner_mode = _determine_runner_mode(
                            sc, runner_arg, project, model, model_json
                        )

                        assert actual_runner_mode == runner_arg
                        sc.log.info.assert_not_called()


def test_project_mode_is_honored():
    # Ensures that a project runner mode is honored when set.

    for project_runner_mode in (RunnerMode.CLOUD, RunnerMode.HYBRID):
        project = MagicMock(runner_mode=project_runner_mode)

        session_runner_modes = (
            list(RunnerMode)
            if project_runner_mode != RunnerMode.CLOUD
            else [RunnerMode.CLOUD]
        )
        for session_default in session_runner_modes:
            sc = MagicMock(session=MagicMock(default_runner=session_default))

            for model_runner_mode in [None, project_runner_mode]:
                model = (
                    MagicMock(runner_mode=model_runner_mode)
                    if model_runner_mode
                    else None
                )

                model_jsons = [None]
                if model_runner_mode:
                    model_jsons.append({"runner_mode": model_runner_mode})

                for model_json in model_jsons:
                    sc.reset_mock()
                    actual_runner_mode = _determine_runner_mode(
                        sc, None, project, model, model_json
                    )

                    assert actual_runner_mode == project_runner_mode
                    if session_default == project_runner_mode or model_json:
                        sc.log.info.assert_not_called()
                    else:
                        sc.log.info.assert_called()


def test_cloud_project_mode_requires_consent():
    # Ensures that a cloud project runner mode requires explicit consent.

    project = MagicMock(runner_mode=RunnerMode.CLOUD)

    session_runner_modes = [RunnerMode.LOCAL, RunnerMode.MANUAL, RunnerMode.HYBRID]
    for session_default in session_runner_modes:
        sc = MagicMock(session=MagicMock(default_runner=session_default))

        for with_model in (False, True):
            model = MagicMock(runner_mode=RunnerMode.CLOUD) if with_model else None

            with pytest.raises(click.Abort):
                _determine_runner_mode(sc, None, project, model, None)


def test_session_default_is_honored_when_possible():
    # Ensures that a cloud project runner mode requires explicit consent.

    project = MagicMock(runner_mode=None)

    for session_default in RunnerMode:
        sc = MagicMock(session=MagicMock(default_runner=session_default))

        for model_runner_mode in [None] + list(RunnerMode):
            model = (
                MagicMock(runner_mode=model_runner_mode) if model_runner_mode else None
            )

            sc.reset_mock()

            model_runner_modes = (
                _model_runner_modes(model) if model else [session_default]
            )

            actual_runner_mode = _determine_runner_mode(sc, None, project, model, None)

            if session_default in model_runner_modes:
                assert actual_runner_mode == session_default
                sc.log.info.assert_not_called()
            else:
                assert actual_runner_mode == model_runner_modes[0]
                sc.log.info.assert_called()


def test_model_json_entry_is_always_honored():
    # Ensures that a cloud project runner mode requires explicit consent.

    project = MagicMock(runner_mode=None)

    for session_default in RunnerMode:
        sc = MagicMock(session=MagicMock(default_runner=session_default))

        for model_runner_mode in [
            RunnerMode.MANUAL,
            RunnerMode.CLOUD,
            RunnerMode.HYBRID,
        ]:
            model = MagicMock(runner_mode=model_runner_mode)
            model_json = {"runner_mode": model_runner_mode.value}

            sc.reset_mock()

            actual_runner_mode = _determine_runner_mode(
                sc, None, project, model, model_json
            )

            expected_runner_mode = (
                RunnerMode.LOCAL
                if model_runner_mode == RunnerMode.MANUAL
                else model_runner_mode
            )

            assert actual_runner_mode == expected_runner_mode
            sc.log.info.assert_not_called()
