from __future__ import annotations

import sys
from pathlib import Path


def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = get_root_dir(SCRIPT_DIR)
TEST_DIR = ROOT_DIR / "experiment" / "debug_tests" / "repair_correctness"

sys.path.append(str(TEST_DIR))

from correctness_lib import get_data_item, put_data_items  # noqa: E402


DEFAULT_ITEMS = {
    "rc_hot": "0",
    "rc_tail": "seed:0",
    "rc_guard": "0",
    "rc_audit": "seed:audit",
    "rc_result": "seed:result",
}


def create_repair_correctness_dataset() -> None:
    put_data_items(DEFAULT_ITEMS)
    for key in sorted(DEFAULT_ITEMS):
        item = get_data_item(key)
        print(f"{key}={item['value']} version={item['version']}")


if __name__ == "__main__":
    create_repair_correctness_dataset()
    print("repair_correctness dataset created successfully.")
