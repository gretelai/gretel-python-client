import json

from typing import Callable

from gretel_client.cli.utils.report_utils import generate_summary_from_legacy


def test_generate_summary_from_legacy_ppl_none_sqs(get_fixture: Callable):
    report_dump = get_fixture("ctgan_legacy_report_json.json")
    report_dict = json.loads(report_dump.read_text())
    summary = generate_summary_from_legacy(report_dict)
    assert len(summary) == 1
    # Privacy protection level will NOT be in the summary
    assert len(summary["summary"]) == 4


def test_generate_summary_from_legacy_missing_ppl_sqs(get_fixture: Callable):
    report_dump = get_fixture("ctgan_legacy_report_json.json")
    report_dict = json.loads(report_dump.read_text())
    del report_dict["privacy_protection_level"]

    summary = generate_summary_from_legacy(report_dict)
    assert len(summary) == 1
    # Privacy protection level will NOT be in the summary
    assert len(summary["summary"]) == 4


def test_generate_summary_from_legacy_multiple_missing_sqs(get_fixture: Callable):
    report_dump = get_fixture("ctgan_legacy_report_json.json")
    report_dict = json.loads(report_dump.read_text())
    del report_dict["field_distribution_stability"]

    summary = generate_summary_from_legacy(report_dict)
    assert len(summary) == 1
    # Privacy protection level will NOT be in the summary, nor will field_distribution_stability
    assert len(summary["summary"]) == 3
