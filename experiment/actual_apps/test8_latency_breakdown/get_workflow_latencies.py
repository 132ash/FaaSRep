import boto3
import logging
import json
import sys

from pathlib import Path
script_dir = Path(__file__).parent
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

import pandas as pd
import multiprocessing
import requests
import config.config as config
from experiment.common import repository, client_logs
from experiment.common import generate_param
repo = repository.Repository()
with open( script_dir / 'successful_txs_travel_reservation.json', 'r') as f:
    tx_list = json.load(f)
all_latencies = repo.get_all_latencies_for_txs(tx_list)
all_latencies_df = pd.DataFrame.from_dict(all_latencies, orient='index')
all_latencies_df.to_csv(script_dir / 'all_latencies_travel_reservation.csv')
