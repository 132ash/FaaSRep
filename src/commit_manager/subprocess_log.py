import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / 'config'))
from experiment_logging import make_experiment_logger


# 配置validator logging模块
def setup_validator_logger(workflow_name, validator_id):
    component = f'{workflow_name}_validator_{validator_id}'
    return make_experiment_logger(component, component)

def log_message(logger, message):
    logger.info(message)
    # 强制刷新缓冲区
    for handler in logger.handlers:
        handler.flush()
