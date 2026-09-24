from pathlib import Path
import yaml

EXPECTED_JOBS={"mro_source_simulator","mro_analytics_pipeline"}

def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def test_bundle_has_six_blue_green_targets(repo_root: Path):
    bundle=load_yaml(repo_root/'databricks.yml')
    targets=bundle['targets']
    assert set(targets)=={
        'dev_blue','dev_green','staging_blue','staging_green','prod_blue','prod_green'
    }
    branches={'dev':'dev','staging':'staging','prod':'main'}
    catalogs={'dev':'mro_dev','staging':'mro_staging','prod':'mro_prod'}
    for env in branches:
        for slot in ('blue','green'):
            t=targets[f'{env}_{slot}']
            assert t['git']['branch']==branches[env]
            assert t['variables']['deployment_environment']==env
            assert t['variables']['deployment_slot']==slot
            assert t['variables']['catalog']==catalogs[env]
            assert t['presets']['trigger_pause_status']=='PAUSED'
            assert 'host' not in t['workspace']
            assert t['workspace']['root_path']=='/Workspace/Shared/.bundle/${bundle.name}/${bundle.target}'
    assert 'workspace_host' not in bundle['variables']
    assert len(set(catalogs.values()))==3

def test_prod_runs_as_service_principal(repo_root: Path):
    targets=load_yaml(repo_root/'databricks.yml')['targets']
    for slot in ('blue','green'):
        assert targets[f'prod_{slot}']['run_as']['service_principal_name']=='${var.run_as_service_principal}'
    for env in ('dev','staging'):
        for slot in ('blue','green'):
            assert 'run_as' not in targets[f'{env}_{slot}']

def test_bundle_direct_engine_and_lock(repo_root: Path):
    bundle=load_yaml(repo_root/'databricks.yml')['bundle']
    assert bundle['engine']=='direct'
    assert bundle['deployment']['lock']['enabled'] is True

def test_resources_include_jobs_and_genie(repo_root: Path):
    manifests={p.name for p in (repo_root/'resources').glob('*.yml')}
    assert manifests=={'mro_simulator.job.yml','mro_analytics.job.yml','mro_genie.yml'}
    jobs={}
    for name in ('mro_simulator.job.yml','mro_analytics.job.yml'):
        jobs.update(load_yaml(repo_root/'resources'/name)['resources']['jobs'])
    assert set(jobs)==EXPECTED_JOBS
    genie=load_yaml(repo_root/'resources'/'mro_genie.yml')['resources']['genie_spaces']
    assert 'mro_analytics_genie' in genie

def test_all_job_notebook_paths_resolve(repo_root: Path):
    for manifest in (repo_root/'resources').glob('*.yml'):
        doc=load_yaml(manifest)
        for job in doc.get('resources',{}).get('jobs',{}).values():
            for task in job.get('tasks',[]):
                p=(manifest.parent/task['notebook_task']['notebook_path']).resolve()
                assert p.exists(), p
