from pathlib import Path
import yaml

def text(repo_root: Path, rel: str)->str:
    return (repo_root/rel).read_text(encoding='utf-8')

def test_selected_silver_and_gold_use_merge(repo_root: Path):
    for rel in [
        'src/data_engineering/03_silver_master.py',
        'src/data_engineering/04_silver_stock.py',
        'src/data_engineering/05_gold_dimensions.py',
        'src/data_engineering/06_gold_mro.py',
        'src/data_engineering/06_gold_stock.py',
        'src/data_engineering/06_gold_procurement.py',
        'src/data_engineering/06_gold_finance.py',
    ]:
        assert 'MERGE INTO' in text(repo_root,rel), rel

def test_scd2_dimensions_and_single_current_gate(repo_root: Path):
    dims=text(repo_root,'src/data_engineering/05_gold_dimensions.py')
    gate=text(repo_root,'src/data_engineering/08_quality_gate.py')
    for d in ('dim_supplier','dim_material','dim_asset'):
        assert f"scd2('{d}'" in dims
        assert f'("{d}"' in gate
    for col in ('effective_from','effective_to','is_current','attr_hash'):
        assert col in dims

def test_expectations_and_observability_assets(repo_root: Path):
    expectations=text(repo_root,'src/data_engineering/08_expectations.py')
    observability=text(repo_root,'src/data_engineering/09_observability.py')
    assert 'EXPECTATIONS' in expectations
    assert 'expectation_result' in expectations
    assert 'asset_observation' in observability
    assert 'row_count' in observability
    assert 'freshness_minutes' in observability

def test_process_mining_uses_transition_log(repo_root: Path):
    pm=text(repo_root,'src/analytics/09_process_mining.py')
    assert 'entity_state_transition_raw' in pm
    for model in ('process_event','process_case_summary','process_transition_summary'):
        assert model in pm

def test_genie_references_all_metric_views(repo_root: Path):
    doc=yaml.safe_load(text(repo_root,'resources/mro_genie.yml'))
    raw=doc['resources']['genie_spaces']['mro_analytics_genie']['serialized_space']
    for cube in (
        'maintenance_metrics','maintenance_material_metrics','inventory_metrics','inventory_position_metrics',
        'procurement_metrics','receiving_metrics','invoice_metrics','fiscal_item_metrics','payables_metrics','payment_metrics'
    ):
        assert cube in raw
