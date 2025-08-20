import sys
import time
from pathlib import Path
import docker

script_dir = Path(__file__).parent
metrics_dir = script_dir / 'metrics'
metrics_dir.mkdir(exist_ok=True)


def collect_container_metrics():
    """子进程：采集容器指标。"""
    container_metrics_file = metrics_dir / 'container_metrics.csv'
    
    # 进程启动时，清空文件并写入表头
    try:
        with open(container_metrics_file, 'w') as f:
            f.write("container_mem_mb\n")
    except IOError as e:
        print(f"无法写入容器指标文件: {e}", file=sys.stderr)
        return

    docker_client = docker.from_env()
    try:
        containers = docker_client.containers.list(filters={'label': 'workflow'})
        total_mem = 0
        for c in containers:
            print(f"Collecting metrics for container: {c.name}")
            stats = c.stats(stream=False)
            total_mem += stats.get('memory_stats', {}).get('usage', 0)
        
        # 计算所有容器的平均内存
        avg_mem_mb = (total_mem / len(containers)) / (1024 * 1024) # MB

        # 将平均值追加到文件，不带时间戳
        with open(container_metrics_file, 'a') as f:
            f.write(f"{avg_mem_mb:.2f}\n")
    except Exception as e:
        print(f"采集容器指标时出错: {e}", file=sys.stderr)

if __name__ == '__main__':
    collect_container_metrics()
