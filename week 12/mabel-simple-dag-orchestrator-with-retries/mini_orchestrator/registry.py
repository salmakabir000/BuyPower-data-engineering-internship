"""
Discovers DAG objects the same way Airflow does: scan a directory of
Python files, import each one, and collect any module-level variable
that is a DAG instance.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from typing import List

from .dag import DAG

logger = logging.getLogger("mini_orchestrator.registry")


def discover_dags(dags_dir: str) -> List[DAG]:
    dags: List[DAG] = []
    path = Path(dags_dir)
    if not path.exists():
        logger.warning("event=dags_dir_missing path=%s", dags_dir)
        return dags

    for py_file in sorted(path.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        module_name = f"mini_orchestrator_dags.{py_file.stem}"
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, DAG):
                dags.append(obj)
                logger.info("event=dag_discovered dag=%s file=%s", obj.name, py_file.name)

    return dags
