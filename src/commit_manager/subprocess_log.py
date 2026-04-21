import logging
import sys

sys.path.append('../../config')
import config
from logging_utils import RunAwareFileHandler


# 配置validator logging模块
def setup_validator_logger(workflow_name, validator_id):
    logger = logging.getLogger(f'{workflow_name}_validator_{validator_id}')

    logger.setLevel(logging.INFO)
    handler = RunAwareFileHandler(config.ROOT_DIR, f"{workflow_name}_validator_{validator_id}.log")
    handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    # 添加处理器到logger
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger

def log_message(logger, message):
    logger.info(message)
    # 强制刷新缓冲区
    for handler in logger.handlers:
        handler.flush()