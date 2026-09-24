from pathlib import Path

def read(repo_root: Path,name: str)->str:
    return (repo_root/'.github/workflows'/name).read_text(encoding='utf-8')

def test_only_ci_and_cd_exist(repo_root: Path):
    assert {p.name for p in (repo_root/'.github/workflows').glob('*.yml')}=={'ci.yml','cd.yml'}

def test_ci_is_fully_static_and_credential_free(repo_root: Path):
    text=read(repo_root,'ci.yml')
    assert 'python -m compileall' in text
    assert 'pytest -q tests/architecture' in text
    assert 'yaml.safe_load' in text
    assert 'bash -n scripts/cd/blue_green.sh' in text

    assert 'id-token: write' not in text
    assert 'environment: dev' not in text
    assert 'DATABRICKS_HOST' not in text
    assert 'DATABRICKS_CLIENT_ID' not in text
    assert 'databricks/setup-cli' not in text
    assert 'databricks bundle validate' not in text


def test_cd_fails_fast_on_auth_and_slot_resolution(repo_root: Path):
    text=read(repo_root,'cd.yml')
    assert 'databricks current-user me' in text
    assert 'DATABRICKS_HOST:?GitHub Environment variable DATABRICKS_HOST is required' in text
    assert 'DATABRICKS_CLIENT_ID:?GitHub Environment variable DATABRICKS_CLIENT_ID is required' in text
    assert 'discovery="$(scripts/cd/blue_green.sh discover "$DELIVERY_ENV")"' in text
    assert 'Invalid CANDIDATE_SLOT' in text
    assert 'target="${DELIVERY_ENV}_${CANDIDATE_SLOT}"' in text

def test_cd_routes_three_branches(repo_root: Path):
    text=read(repo_root,'cd.yml')
    for branch in ('dev','staging','main'):
        assert f'refs/heads/{branch}' in text
    for env in ('dev','staging','prod'):
        assert f'environment={env}' in text

def test_cd_uses_oidc_and_isolated_catalog(repo_root: Path):
    text=read(repo_root,'cd.yml')
    assert 'DATABRICKS_AUTH_TYPE: github-oidc' in text
    assert 'databricks catalogs create' in text
    assert 'databricks catalogs delete' in text
    assert 'mro_ci_${DELIVERY_ENV}' in text
    assert 'ops_schema=ops' in text

def test_cd_passes_service_principal_bundle_variable(repo_root: Path):
    text=read(repo_root,'cd.yml')
    assert 'run_as_service_principal=$DATABRICKS_CLIENT_ID' in text

def test_helper_has_dev_marker_and_runtime_promotion(repo_root: Path):
    text=(repo_root/'scripts/cd/blue_green.sh').read_text(encoding='utf-8')
    assert 'discover_dev' in text
    assert 'discover_runtime' in text
    assert 'promote_dev' in text
    assert 'promote_runtime' in text
    dev=text.split('promote_dev()',1)[1].split('promote_runtime()',1)[0]
    assert 'unpause_slot' not in dev
    runtime=text.split('promote_runtime()',1)[1].split('promote()',1)[0]
    assert 'unpause_slot' in runtime

def test_deprecated_deploy_workflows_absent(repo_root: Path):
    wf=repo_root/'.github/workflows'
    assert not (wf/'deploy-dev.yml').exists()
    assert not (wf/'deploy-prod.yml').exists()
