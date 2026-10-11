# Run from contracts/:  python -m pytest
"""The runner's preparation of a contract (no AWS)."""

from check import errors, only_not_evaluated, quality_env, with_staging_dir


def test_staging_dir_is_added_to_athena_servers_only():
    contract = {
        "servers": [
            {"server": "athena", "type": "athena", "schema": "thelook_gold"},
            {"server": "other", "type": "s3"},
        ]
    }
    out = with_staging_dir(contract, "s3://bucket/athena-results/")
    assert out["servers"][0]["stagingDir"] == "s3://bucket/athena-results/"
    assert "stagingDir" not in out["servers"][1]
    # The committed contract is not modified.
    assert "stagingDir" not in contract["servers"][0]


def test_checks_use_the_quality_key_not_the_process_identity(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "batch-key")
    monkeypatch.setenv("QUALITY_AWS_ACCESS_KEY_ID", "quality-key")
    monkeypatch.setenv("QUALITY_AWS_SECRET_ACCESS_KEY", "quality-secret")
    env = quality_env()
    assert env["DATACONTRACT_S3_ACCESS_KEY_ID"] == "quality-key"
    assert env["DATACONTRACT_S3_SECRET_ACCESS_KEY"] == "quality-secret"


REPORT = """some table
🔴 data contract is invalid, found the following errors:
1) orders Check that field status has invalid_count = 0: Actual invalid_count(status) was 3
2) orders Check that unique field id has no duplicate values: Error evaluating check: throttled
"""


def test_errors_are_the_numbered_summary_lines():
    found = errors(REPORT)
    assert len(found) == 2 and found[0].startswith("1) orders")


def test_only_checks_that_could_not_run_are_retried():
    assert only_not_evaluated(["1) x: Error evaluating check: throttled"])
    assert only_not_evaluated(["1) x: Could not read model 'orders': timeout"])
    # A real failure among them: report it, do not retry.
    assert not only_not_evaluated(errors(REPORT))
    assert not only_not_evaluated([])
