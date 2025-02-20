import subprocess
import time
import numpy as np
import logging
import os
import random
from datetime import datetime

# 全局配置
CONFIG = {
    'total_nodes': 5,
    'cpus_per_node': 96,      # 每节点96核心
    'mem_per_node': 300,      # 每节点内存300GB
    'interval': 300,          # 5分钟提交一次
    'max_jobs': None,
    
    # 工作时间配置（周一至周五 9:00-17:00）
    'working_hours': {
        'node_usage': 0.8,     
        'node_std': 0.3,       
        'cpu_usage': 0.08,     # 平均使用8个核心 (96 * 0.08 ≈ 8)
        'mem_usage': 0.15,     # 内存使用率与CPU使用率相同
        'mean_duration': 10,   # 10分钟
    },
    
    # 非工作时间配置
    'non_working_hours': {
        'node_usage': 0.4,     
        'node_std': 0.4,       
        'cpu_usage': 0.08,      # 平均使用8个核心
        'mem_usage': 0.2,      # 内存使用率与CPU使用率相同
        'mean_duration': 15,   # 15分钟
    },
    
    # 突发任务配置
    'burst': {
        'probability': 0.03,   
        'node_multiplier': 1.5,
        'cpu_multiplier': 1.2, # 突发任务使用约10个核心 (8 * 1.2 ≈ 10)
        'mem_multiplier': 1.2, # 内存倍数与CPU倍数相同
        'min_jobs': 2,        
        'max_jobs': 3,        
    }
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def submit_job(num_nodes, num_cpus_per_node, duration_minutes, job_name):
    """提交一个任务到集群"""
    try:
        # 时间限制设置为预期执行时间的2倍
        time_limit_minutes = duration_minutes * 2
        hours = time_limit_minutes // 60
        mins = time_limit_minutes % 60
        time_limit = f"{hours}:{mins}:00"
        
        # 计算内存请求（GB）
        # 假设每个CPU核心对应2.5GB内存
        mem_per_task = int(num_cpus_per_node * 2.5)
        
        cmd = [
            "/root/CraneSched-FrontEnd/build/bin/cbatch",
            "-N", str(num_nodes),          # 节点数
            "-J", job_name,                # 任务名
            "-t", time_limit,              # 运行时限
            "-c", str(num_cpus_per_node),  # 每个任务的CPU数
            "--mem", f"{mem_per_task}G",   # 内存请求
            "--ntasks-per-node", "1",      # 每个节点运行1个任务
            "--wrap", f"sleep {duration_minutes}m"
        ]
        
        subprocess.run(cmd, check=True)
        logging.info(f"Submitted job {job_name}: nodes={num_nodes}, cpus={num_cpus_per_node}, mem={mem_per_task}GB, actual_duration={duration_minutes}m, time_limit={time_limit}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to submit job {job_name}: {e}")

def generate_workload_pattern(time_of_day, day_of_week):
    """根据时间生成工作负载模式"""
    # 工作时间（周一至周五 9:00-17:00）负载较高
    is_working_hours = 9 <= time_of_day.hour <= 17
    is_weekday = day_of_week < 5
    
    if is_working_hours and is_weekday:
        config = CONFIG['working_hours']
        mean_nodes = CONFIG['total_nodes'] * config['node_usage']
        std_nodes = mean_nodes * config['node_std']
        mean_duration = config['mean_duration']
        mean_cpus = CONFIG['cpus_per_node'] * config['cpu_usage']
    else:
        config = CONFIG['non_working_hours']
        mean_nodes = CONFIG['total_nodes'] * config['node_usage']
        std_nodes = mean_nodes * config['node_std']
        mean_duration = config['mean_duration']
        mean_cpus = CONFIG['cpus_per_node'] * config['cpu_usage']
    
    return mean_nodes, std_nodes, mean_duration, mean_cpus

def generate_jobs():
    """生成并提交测试任务"""
    job_counter = 0
    
    while True:
        current_time = datetime.now()
        
        # 获取当前时间的负载模式
        mean_nodes, std_nodes, mean_duration, mean_cpus = generate_workload_pattern(
            current_time,
            current_time.weekday()
        )
        
        # 随机生成突发任务
        if random.random() < CONFIG['burst']['probability']:
            num_burst_jobs = random.randint(
                CONFIG['burst']['min_jobs'],
                CONFIG['burst']['max_jobs']
            )
            for _ in range(num_burst_jobs):
                # 限制节点数最多为2
                num_nodes = min(2, max(1, int(round(np.random.normal(1.5, 0.5)))))
                
                num_cpus = max(1, min(
                    int(round(np.random.normal(
                        mean_cpus * CONFIG['burst']['cpu_multiplier'],
                        2
                    ))),
                    CONFIG['cpus_per_node']
                ))
                duration = max(5, int(round(np.random.normal(30, 10))))
                submit_job(num_nodes, num_cpus, duration, f"burst-job-{job_counter}")
                job_counter += 1
        
        # 生成常规任务
        # 80%概率使用1个节点，20%概率使用2个节点
        num_nodes = 2 if random.random() < 0.2 else 1
        
        # 生成CPU请求数
        num_cpus = max(1, min(
            int(round(np.random.normal(mean_cpus, 2))),
            CONFIG['cpus_per_node']
        ))
        
        # 生成更多样化的运行时间
        duration_patterns = [
            (0.6, mean_duration, 20),      # 60%常规任务
            (0.3, mean_duration * 2, 40),  # 30%长任务
            (0.1, mean_duration * 0.5, 10) # 10%短任务
        ]
        
        pattern = random.choices(
            duration_patterns,
            weights=[p[0] for p in duration_patterns]
        )[0]
        duration = max(5, int(round(np.random.normal(pattern[1], pattern[2]))))
        
        submit_job(num_nodes, num_cpus, duration, f"test-job-{job_counter}")
        job_counter += 1
        
        if CONFIG['max_jobs'] and job_counter >= CONFIG['max_jobs']:
            logging.info(f"Reached maximum number of jobs ({CONFIG['max_jobs']})")
            break
        
        time.sleep(CONFIG['interval'])

def main():
    logging.info("Starting job generator")
    logging.info(f"Total nodes in cluster: {CONFIG['total_nodes']}")
    logging.info(f"CPUs per node: {CONFIG['cpus_per_node']}")
    logging.info(f"Submission interval: {CONFIG['interval']} seconds")
    logging.info(f"Max jobs: {CONFIG['max_jobs'] if CONFIG['max_jobs'] else 'unlimited'}")
    
    try:
        generate_jobs()
    except KeyboardInterrupt:
        logging.info("Job generator stopped by user")
    except Exception as e:
        logging.error(f"Job generator failed: {e}")

if __name__ == "__main__":
    main()
