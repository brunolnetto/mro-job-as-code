from pathlib import Path

def read(repo_root: Path,name: str)->str:
    return (repo_root/'.github/workflows'/name).read_text(encoding='utf-8')

def test_only_ci_and_cd_exist(repo_root: Path):
    assert {p.name for p in (repo_root/'.github/workflows').glob('*.yml')}=={'ci.yml','cd.yml'}

def test_ci_validates_python_tests_and_bundle(repo_root: Path):
    text=read(repo_root,'ci.yml')
    assert 'python -m compileall' in text
    assert 'pytest -q tests/architecture' in text
    assert 'databricks/setup-cli' in text
    assert 'databricks bundle validate' in text
    for target in ('dev_blue','dev_green','staging_blue','staging_green','prod_blue','prod_green'):
        assert target in text


def test_ci_keeps_oidc_away_from_pull_request_code(repo_root: Path):
    text=read(repo_root,'ci.yml')
    workflow_permissions=text.split('jobs:',1)[0]
    assert 'id-token: write' not in workflow_permissions

    static=text.split('  static-validation:',1)[1].split('  bundle-validation:',1)[0]
    assert 'id-token: write' not in static
    assert 'environment: dev' not in static
    assert 'DATABRICKS_HOST' not in static
    assert 'DATABRICKS_CLIENT_ID' not in static
    assert 'pytest -q tests/architecture' in static

    authenticated=text.split('  bundle-validation:',1)[1]
    assert "if: github.event_name == 'push'" in authenticated
    assert 'environment: dev' in authenticated
    assert 'id-token: write' in authenticated
    assert 'DATABRICKS_AUTH_TYPE: github-oidc' in authenticated
    assert 'pytest -q tests/architecture' not in authenticated
    assert 'python -m compileall' not in authenticated

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
