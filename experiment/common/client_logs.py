import logging
import sys

def setup_logging_for_process(script_dir, client_id):
    """为每个子进程配置独立的日志文件。"""
    log_dir = script_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"client_{client_id}.log"
    
    # 移除旧的 handlers，为子进程设置新的
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [Client-{client_id}] [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler(sys.stdout) # 也可以同时输出到控制台
        ]
    )