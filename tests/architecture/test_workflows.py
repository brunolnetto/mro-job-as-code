from pathlib import Path

def read(repo_root: Path,name: str)->str:
    return (repo_root/'.github/workflows'/name).read_text(encoding='utf-8')

def test_only_ci_and_cd_exist(repo_root: Path):
    assert {p.name for p in (repo_root/'.github/workflows').glob('*.yml')}=={'ci.yml','cd.yml'}

def test_ci_is_fully_static_and_credential_free(repo_root: Path):
    text=read(repo_root,'ci.yml')
    assert 'branches: [dev, main]' in text
    assert 'python -m compileall' in text
    assert 'pytest -q tests/architecture' in text
    assert 'yaml.safe_load' in text
    assert 'bash -n scripts/cd/*.sh' in text

    assert 'staging' not in text
    assert 'id-token: write' not in text
    assert 'environment: dev' not in text
    assert 'DATABRICKS_HOST' not in text
    assert 'DATABRICKS_CLIENT_ID' not in text
    assert 'databricks/setup-cli' not in text
    assert 'databricks bundle validate' not in text

def test_cd_routes_only_dev_and_prod(repo_root: Path):
    text=read(repo_root,'cd.yml')

    assert 'branches: [dev, main]' in text
    assert 'refs/heads/dev' in text
    assert 'refs/heads/main' in text
    assert 'environment=dev' in text
    assert 'environment=prod' in text

    assert 'refs/heads/staging' not in text
    assert 'staging' not in text

def test_cd_uses_single_ephemeral_dev_target(repo_root: Path):
    text=read(repo_root,'cd.yml')

    assert 'target=dev' in text
    assert 'candidate_slot=ephemeral' in text
    assert 'assert-status dev ephemeral PAUSED' in text
    assert 'DEV validation complete; schedules remain PAUSED' in text
    assert 'discover dev' not in text
    assert 'promote dev' not in text

def test_cd_keeps_blue_green_only_for_prod(repo_root: Path):
    text=read(repo_root,'cd.yml')

    assert 'scripts/cd/blue_green.sh discover prod' in text
    assert 'target="prod_${CANDIDATE_SLOT}"' in text
    assert 'scripts/cd/blue_green.sh promote prod' in text
    assert 'Rollback is only supported for PROD' in text

def test_cd_uses_oidc_and_isolated_catalog(repo_root: Path):
    text=read(repo_root,'cd.yml')

    assert 'DATABRICKS_AUTH_TYPE: github-oidc' in text
    assert 'databricks current-user me' in text
    assert 'databricks catalogs create' in text
    assert 'databricks catalogs delete' in text
    assert 'mro_ci_${DELIVERY_ENV}' in text
    assert 'ops_schema=ops' in text

def test_cd_passes_service_principal_bundle_variable(repo_root: Path):
    text=read(repo_root,'cd.yml')
    assert 'run_as_service_principal=$DATABRICKS_CLIENT_ID' in text

def test_helper_is_prod_only_for_blue_green(repo_root: Path):
    text=(repo_root/'scripts/cd/blue_green.sh').read_text(encoding='utf-8')

    assert 'discover_prod' in text
    assert 'promote_prod' in text
    assert 'discover_dev' not in text
    assert 'promote_dev' not in text
    assert 'staging' not in text
    assert 'Blue-green discovery is only supported for PROD' in text
    assert 'Blue-green promotion is only supported for PROD' in text

def test_helper_rejects_ambiguous_prod_states(repo_root: Path):
    text=(repo_root/'scripts/cd/blue_green.sh').read_text(encoding='utf-8')
    runtime=text.split('discover_prod()',1)[1].split('discover(){',1)[0]

    assert 'INCONSISTENT' in runtime
    assert 'Uncertain PROD deployment markers while both schedules are paused' in runtime
    assert 'UNPAUSED/PAUSED|UNPAUSED/MISSING' in runtime
    assert 'PAUSED/UNPAUSED|MISSING/UNPAUSED' in runtime
    assert 'ACTIVE/INACTIVE' in runtime
    assert 'INACTIVE/ACTIVE' in runtime
    assert 'INACTIVE/INACTIVE' in runtime

def test_deprecated_deploy_workflows_absent(repo_root: Path):
    wf=repo_root/'.github/workflows'
    assert not (wf/'deploy-dev.yml').exists()
    assert not (wf/'deploy-prod.yml').exists()


def test_prod_deploy_and_rollback_share_concurrency_group(repo_root: Path):
    text=read(repo_root,'cd.yml')
    deploy=text.split('  deploy:',1)[1].split('  rollback:',1)[0]
    rollback=text.split('  rollback:',1)[1]

    assert 'group: mro-cd-${{ needs.context.outputs.environment }}' in deploy
    assert 'cancel-in-progress: false' in deploy
    assert 'group: mro-cd-prod' in rollback
    assert 'cancel-in-progress: false' in rollback
