from pathlib import Path
import yaml

def load_job(repo_root: Path, filename: str, key: str):
    doc=yaml.safe_load((repo_root/'resources'/filename).read_text(encoding='utf-8'))
    return doc['resources']['jobs'][key]

def test_simulator_is_single_operational_task(repo_root: Path):
    job=load_job(repo_root,'mro_simulator.job.yml','mro_source_simulator')
    assert [t['task_key'] for t in job['tasks']]==['simulate_mro']
    assert job['tasks'][0]['notebook_task']['notebook_path'].startswith('../src/operational/')

def test_analytics_job_has_no_simulator_task(repo_root: Path):
    job=load_job(repo_root,'mro_analytics.job.yml','mro_analytics_pipeline')
    keys={t['task_key'] for t in job['tasks']}
    assert 'simulate_mro' not in keys
    assert {'semantic_model','process_mining','expectations','observability','quality_gate'} <= keys

def test_analytics_tail_orders_governance_and_observability(repo_root: Path):
    job=load_job(repo_root,'mro_analytics.job.yml','mro_analytics_pipeline')
    tasks={t['task_key']:t for t in job['tasks']}
    assert 'depends_on' not in tasks['bronze_ingest']
    assert {d['task_key'] for d in tasks['observability']['depends_on']}=={'semantic_model','process_mining','expectations'}
    assert tasks['quality_gate']['depends_on']==[{'task_key':'observability'}]
    assert tasks['process_mining']['notebook_task']['notebook_path'].endswith('src/analytics/09_process_mining.py')
    assert tasks['expectations']['notebook_task']['notebook_path'].endswith('src/data_engineering/08_expectations.py')

def test_jobs_are_slot_addressable(repo_root: Path):
    for filename,key in [('mro_simulator.job.yml','mro_source_simulator'),('mro_analytics.job.yml','mro_analytics_pipeline')]:
        job=load_job(repo_root,filename,key)
        assert '${var.deployment_environment}' in job['name']
        assert '${var.deployment_slot}' in job['name']
        assert job['schedule']['pause_status']=='PAUSED'
