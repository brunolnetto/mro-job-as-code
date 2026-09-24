from pathlib import Path
import yaml

EXPECTED_JOBS={"mro_source_simulator","mro_analytics_pipeline"}

def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def test_bundle_has_single_dev_and_prod_blue_green_targets(repo_root: Path):
    bundle=load_yaml(repo_root/'databricks.yml')
    targets=bundle['targets']

    assert set(targets)=={'dev','prod_blue','prod_green'}

    dev=targets['dev']
    assert dev['git']['branch']=='dev'
    assert dev['variables']['deployment_environment']=='dev'
    assert dev['variables']['deployment_slot']=='ephemeral'
    assert dev['variables']['deployment_strategy']=='ephemeral'
    assert dev['variables']['catalog']=='mro_dev'
    assert dev['presets']['trigger_pause_status']=='PAUSED'
    assert 'run_as' not in dev

    for slot in ('blue','green'):
        prod=targets[f'prod_{slot}']
        assert prod['git']['branch']=='main'
        assert prod['variables']['deployment_environment']=='prod'
        assert prod['variables']['deployment_slot']==slot
        assert prod['variables']['deployment_strategy']=='blue-green'
        assert prod['variables']['catalog']=='mro_prod'
        assert prod['presets']['trigger_pause_status']=='PAUSED'
        assert prod['run_as']['service_principal_name']=='${var.run_as_service_principal}'

    for target in targets.values():
        assert 'host' not in target['workspace']
        assert target['workspace']['root_path']=='/Workspace/Shared/.bundle/${bundle.name}/${bundle.target}'

    assert 'workspace_host' not in bundle['variables']

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
