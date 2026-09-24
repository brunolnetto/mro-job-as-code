from pathlib import Path


FORBIDDEN_ANALYTICS_CONTROL_TERMS = {
    "committed_tick",
    "sim_state_raw",
    "_source_committed_tick",
    "pipeline_watermark",
}

SIMULATOR_PRIVATE_SEQUENCE_TERMS = {
    "simulation_tick",
    "valid_from_tick",
}


def python_files(path: Path):
    return sorted(path.glob("*.py"))


def test_simulation_control_does_not_leak_into_analytics(repo_root: Path):
    data_engineering = repo_root / "src" / "data_engineering"
    analytics = repo_root / "src" / "analytics"

    for path in [*python_files(data_engineering), *python_files(analytics)]:
        text = path.read_text(encoding="utf-8")
        for term in FORBIDDEN_ANALYTICS_CONTROL_TERMS:
            assert term not in text, f"{term!r} leaked into {path.relative_to(repo_root)}"


def test_private_sequence_is_only_named_at_bronze_boundary(repo_root: Path):
    data_engineering = repo_root / "src" / "data_engineering"
    bronze = data_engineering / "02_bronze_ingest.py"

    # Bronze is allowed to know the private source columns only so it can strip
    # them from the analytics-facing representation.
    bronze_text = bronze.read_text(encoding="utf-8")
    for term in SIMULATOR_PRIVATE_SEQUENCE_TERMS:
        assert term in bronze_text

    for path in python_files(data_engineering):
        if path == bronze:
            continue
        text = path.read_text(encoding="utf-8")
        for term in SIMULATOR_PRIVATE_SEQUENCE_TERMS:
            assert term not in text, f"{term!r} leaked past Bronze into {path.name}"


def test_operational_code_is_not_referenced_by_analytics_job(repo_root: Path):
    analytics_manifest = (
        repo_root / "resources" / "mro_analytics.job.yml"
    ).read_text(encoding="utf-8")
    assert "src/operational" not in analytics_manifest


def test_semantic_code_does_not_reference_operational_schema(repo_root: Path):
    semantic = (repo_root / "src" / "analytics" / "07_semantic_model.py").read_text(
        encoding="utf-8"
    )
    assert "source_schema" not in semantic
    assert "mro_sim" not in semantic
