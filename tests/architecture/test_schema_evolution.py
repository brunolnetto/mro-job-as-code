from pathlib import Path
import re
import yaml


def test_schema_contract_manifest(repo_root: Path):
    doc=yaml.safe_load((repo_root/'contracts/schema_contracts.yml').read_text())
    assert doc['policy']=='additive_only'
    assert {'supplier','material','asset','entity_state_transition_store'} <= set(doc['sources'])


def test_source_required_columns_still_exist(repo_root: Path):
    contract=yaml.safe_load((repo_root/'contracts/schema_contracts.yml').read_text())['sources']
    text=(repo_root/'src/operational/01_simulate_mro.py').read_text()
    for table,spec in contract.items():
        m=re.search(rf'"{re.escape(table)}"\s*:\s*schema\((.*?)\n\s*\),',text,re.S)
        assert m, table
        cols=set(re.findall(r'\("([A-Za-z0-9_]+)"\s*,',m.group(1)))
        assert set(spec['required']) <= cols, (table,set(spec['required'])-cols)


def test_scd2_contract_columns_are_created(repo_root: Path):
    text=(repo_root/'src/data_engineering/05_gold_dimensions.py').read_text()
    for dim in ('dim_supplier','dim_material','dim_asset'):
        assert f"scd2('{dim}'" in text
    for col in ('effective_from','effective_to','is_current','source_commit_version'):
        assert col in text
