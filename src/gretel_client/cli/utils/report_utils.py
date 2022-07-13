# PROD-76 All legacy fields from existing report types used in summaries.
SIMPLE_LEGACY_FIELDS = [
    "elapsed_time_seconds",
    "record_count",
    "field_count",
    "warnings",
    "training_time_seconds",
    "field_transforms",
    "value_transforms",
    "run_time_seconds",
]

SQS_LEGACY_FIELDS = [
    "synthetic_data_quality_score",
    "field_correlation_stability",
    "principal_component_stability",
    "field_distribution_stability",
    "privacy_protection_level",
]


def generate_summary_from_legacy(report_dict) -> dict:
    """
    All reports should have a summary section.  Legacy reports exist from before the introduction of
    the summary section.  This utility method will transparently return a summary section from any legacy report dict.
    Args:
        report_dict: dict of report values. We most commonly encounter this format due to reading an artifact
        and json loading it.
    Returns:
        Summary in dict format (`{"summary": [{"field": "field1", "value": "value1"}, ...]}`)
    """
    summary_list = []
    summary_list += [
        {"field": f, "value": report_dict[f]}
        for f in SIMPLE_LEGACY_FIELDS
        if f in report_dict
    ]
    summary_list += [
        {"field": f, "value": report_dict[f]["score"]}
        for f in SQS_LEGACY_FIELDS
        if f in report_dict
    ]
    return {"summary": summary_list}
