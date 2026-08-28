"""
registry.py - finds all @dag-decorated functions inside a folder of .py files,
so the CLI can run/list DAGs by name instead of needing a file path each time.
"""

import importlib.util
from pathlib import Path


def discover_dags(dags_dir="dags"):
    """Returns {dag_name: dag_function} for every @dag function found in
    every .py file inside dags_dir (non-recursive, top-level files only)."""
    dags_dir = Path(dags_dir)
    registry = {}

    if not dags_dir.exists():
        return registry

    for py_file in sorted(dags_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(py_file.stem, py_file)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            print(f"Warning: failed to import '{py_file}': {e}")
            continue

        for obj in vars(module).values():
            if callable(obj) and hasattr(obj, "_mini_orchestrator_dag_name"):
                registry[obj._mini_orchestrator_dag_name] = obj

    return registry
