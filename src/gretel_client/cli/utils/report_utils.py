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

SCORE_FIELDS = [
    "synthetic_data_quality_score",
    "field_correlation_stability",
    "principal_component_stability",
    "field_distribution_stability",
    "privacy_protection_level",
    "semantic_similarity",
    "structure_similarity",
    "column_correlation_stability",
    "deep_structure_stability",
    "column_distribution_stability",
    "text_structure_similarity",
    "text_semantic_similarity",
    "membership_inference_attack_score",
    "attribute_inference_attack_score",
    "data_privacy_score",
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
    for f in SIMPLE_LEGACY_FIELDS:
        if report_dict.get(f) is not None:
            summary_list.append({"field": f, "value": report_dict[f]})
    for f in SCORE_FIELDS:
        # "privacy_protection_level" can have value None.
        if isinstance(report_dict.get(f), dict) and report_dict.get(f).get("score"):
            summary_list.append({"field": f, "value": report_dict[f]["score"]})
    return {"summary": summary_list}
