import ast
import re
from pathlib import Path


FACT_PATTERN = re.compile(r'["\'](fact_[a-z0-9_]+)["\']')


def extract_cube_fact_map(path: Path) -> dict[str, str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "CUBE_FACT_MAP":
                    value = ast.literal_eval(node.value)
                    assert isinstance(value, dict)
                    return value

    raise AssertionError("CUBE_FACT_MAP not found")


def extract_gold_facts(repo_root: Path) -> set[str]:
    facts = set()
    for path in (repo_root / "src" / "data_engineering").glob("06_gold_*.py"):
        text = path.read_text(encoding="utf-8")
        for match in FACT_PATTERN.finditer(text):
            facts.add(match.group(1))
    return facts


def test_every_gold_fact_has_exactly_one_primary_cube(repo_root: Path):
    cube_map = extract_cube_fact_map(repo_root / "src" / "analytics" / "07_semantic_model.py")
    gold_facts = extract_gold_facts(repo_root)

    assert len(gold_facts) == 10
    assert len(cube_map) == 10
    assert len(set(cube_map.values())) == len(cube_map)
    assert set(cube_map.values()) == gold_facts


def test_cube_names_are_unique_and_metric_oriented(repo_root: Path):
    cube_map = extract_cube_fact_map(repo_root / "src" / "analytics" / "07_semantic_model.py")

    assert len(cube_map) == len(set(cube_map))
    assert all(name.endswith("_metrics") for name in cube_map)
