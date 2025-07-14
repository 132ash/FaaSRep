import logging

# 配置validator logging模块
def setup_validator_logger(validator_id):
    logger = logging.getLogger(f'validator_{validator_id}')
    log_file_path = f"/home/ash/FaaSnap/faas/logging/validator_{validator_id}.log"


    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file_path, mode='a')
    handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    
    # 添加处理器到logger
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger

def log_validator_message(logger, message):
    logger.info(message)
    # 强制刷新缓冲区
    for handler in logger.handlers:
        handler.flush()