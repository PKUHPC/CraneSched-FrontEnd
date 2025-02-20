from batsim.batsim import BatsimScheduler
from procset import ProcSet
from flask import Flask, request, jsonify
import threading
import requests

class PowerControlScheduler(BatsimScheduler):
    """实现了FCFS调度算法并集成了电源控制的调度器"""
    
    def __init__(self, options=None):
        super().__init__(options)
        self.power_controller_url = "http://localhost:8081"  # PowerControl的HTTP服务地址
        self.power_check_interval = 1800  # 每30分钟检查一次电源状态
        self.record_interval = 60         # 用于设置下一次回调时间
        self.last_power_check = 0         # 上次电源检查时间
        
        # 添加能耗相关属性
        self.last_energy = 0.0           # 上次能耗值
        self.last_energy_time = 0.0      # 上次能耗时间
        self.current_power = 0.0         # 当前功率
        
        self.waiting_jobs = []           # 等待队列
        self.running_jobs = []           # 运行中的作业
        self.simulation_started = False  # 添加标志，表示是否已经开始接收作业
        
        # 初始化Flask应用
        self.app = Flask(__name__)
        self.setup_routes()
        
        # 启动HTTP服务器
        self.http_thread = threading.Thread(target=self.run_http_server)
        self.http_thread.daemon = True
        self.http_thread.start()

    def setup_routes(self):
        # 获取当前时间
        @self.app.route('/scheduler/time', methods=['GET'])
        def get_current_time():
            return jsonify({
                "current_time": self.bs.time()
            })
        
        # 电源操作接口
        @self.app.route('/scheduler/power/action', methods=['POST'])
        def handle_power_action():
            data = request.json
            node_id = data.get('node_id')
            action = data.get('action')
            
            if not node_id or not action:
                return jsonify({"error": "Missing node_id or action"}), 400
            
            try:
                if action == "wake_up":
                    self.bs.set_resource_state(f"{node_id}-{node_id}", str(0))
                elif action == "sleep":
                    self.bs.set_resource_state(f"{node_id}-{node_id}", str(1))
                elif action == "power_on":
                    self.bs.set_resource_state(f"{node_id}-{node_id}", str(0))
                elif action == "power_off":
                    self.bs.set_resource_state(f"{node_id}-{node_id}", str(1))
                else:
                    return jsonify({"error": f"Unknown action: {action}"}), 400
                
                return jsonify({"success": True})
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
    def run_http_server(self):
        # 使用8082端口，避免与其他服务冲突
        self.app.run(host='0.0.0.0', port=8082)

    def onAfterBatsimInit(self):
        """初始化调度器"""
        super().onAfterBatsimInit()
        
    def onSimulationBegins(self):
        super().onSimulationBegins()
        """模拟开始时的回调"""
        print("Simulation begins at time:", self.bs.time())
        
        # 初始化时间记录
        self.last_power_check = self.bs.time()
        self.simulation_started = False  # 添加标志，表示是否已经开始接收作业
        
        # 设置第一次回调
        self.schedule_next_record()

    def schedule_next_record(self):
        """设置下一次回调的时间（每分钟）"""
        next_check = self.bs.time() + self.record_interval  # 1分钟后
        self.bs.wake_me_up_at(next_check)

    def onRequestedCall(self):
        """响应定时回调"""
        print("onRequestedCall is called at time:", self.bs.time())
        current_time = self.bs.time()
        
        # 检查是否需要执行电源管理（每30分钟）
        if current_time >= self.last_power_check + self.power_check_interval:
            # 通过HTTP调用执行电源管理
            try:
                requests.post(f"{self.power_controller_url}/power/actions/execute")
                self.last_power_check = current_time
            except Exception as e:
                print(f"Failed to execute power actions: {e}")
        
        # 记录系统状态
        try:
            requests.post(
                f"{self.power_controller_url}/system/state",
                json={
                    "current_time": current_time,
                    "running_jobs": self.running_jobs,
                    "waiting_jobs": self.waiting_jobs,
                    "current_power": self.current_power
                }
            )
        except Exception as e:
            print(f"Failed to record system state: {e}")
        
        # 如果已经开始接收作业，且没有运行或等待的作业，则不再设置回调
        if self.simulation_started and not (self.running_jobs or self.waiting_jobs):
            print(f"[Time {current_time}] All jobs completed, stopping callbacks")
            return
        
        self.schedule_next_record()

    def onJobSubmission(self, job):
        """处理作业提交"""
        self.simulation_started = True  # 标记已经开始接收作业
        
        if job.requested_resources > self.bs.nb_resources:
            self.bs.reject_jobs([job])
            return
            
        self.waiting_jobs.append(job)
        print(f"[Time {self.bs.time()}] Job {job.id} submitted. "
              f"Waiting jobs: {len(self.waiting_jobs)}, Running jobs: {len(self.running_jobs)}")
        # self.try_schedule_jobs()
        self.energy_aware_schedule_jobs()

    def onJobCompletion(self, job):
        """处理作业完成"""
        if job in self.running_jobs:
            self.running_jobs.remove(job)
            
        # 通知节点作业已完成
        for node in job.allocation:
            try:
                requests.delete(f"{self.power_controller_url}/nodes/{node}/jobs")
            except Exception as e:
                print(f"Failed to remove job from node {node}: {e}")
        
        print(f"[Time {self.bs.time()}] Job {job.id} completed. "
              f"Allocation: {job.allocation}, "
              f"Waiting jobs: {len(self.waiting_jobs)}, Running jobs: {len(self.running_jobs)}")
        
        # self.try_schedule_jobs()
        self.energy_aware_schedule_jobs()

    def onJobKilled(self, job):
        """处理作业被杀死"""
        assert False, "onJobKilled is called"
        if job in self.running_jobs:
            self.running_jobs.remove(job)
        for node in job.allocation:
            try:
                requests.delete(f"{self.power_controller_url}/nodes/{node}/jobs")
            except Exception as e:
                print(f"Failed to remove job from node {node}: {e}")

    def onJobMessage(self, timestamp, job, message):
        """处理来自作业的消息"""
        print(f"Message from job {job.id} at time {timestamp}: {message}")

    def onMachinePStateChanged(self, machines, new_pstate):
        """处理节点电源状态变化"""
        for machine in machines:
            try:
                requests.post(
                    f"{self.power_controller_url}/power/state/transition",
                    json={
                        "machine": machine,
                        "new_state": new_pstate
                    }
                )
            except Exception as e:
                print(f"Failed to handle state transition: {e}")

    def onReportEnergyConsumed(self, consumed_energy):
        """处理能耗报告"""
        current_time = self.bs.time()
        
        # 计算功率
        if self.last_energy_time > 0:  # 不是第一次更新
            time_diff = current_time - self.last_energy_time
            energy_diff = consumed_energy - self.last_energy
            
            if time_diff > 0:
                self.current_power = energy_diff / time_diff  # 计算功率（瓦特）
        
        self.last_energy = consumed_energy
        self.last_energy_time = current_time

    def onDeadlock(self):
        """处理死锁情况"""
        print("Deadlock detected at time:", self.bs.time())
        raise ValueError("Batsim has reached a deadlock")

    def onSimulationEnds(self):
        """模拟结束时的回调"""
        print("Simulation ends at time:", self.bs.time())
        super().onSimulationEnds()

    def try_schedule_jobs(self):
        """尝试调度等待队列中的作业（FCFS算法 + 负载均衡）"""
        if not self.waiting_jobs:
            return
        
        print(f"\n[Time {self.bs.time()}] Attempting to schedule jobs...")
        print(f"Current state: Waiting jobs: {len(self.waiting_jobs)}, Running jobs: {len(self.running_jobs)}")
            
        # 获取可用节点
        try:
            response = requests.get(f"{self.power_controller_url}/nodes/available")
            available_nodes = response.json()["nodes"]
            if not available_nodes:
                print(f"[Time {self.bs.time()}] No available nodes for scheduling")
                return
        except Exception as e:
            print(f"Failed to get available nodes: {e}")
            return
        
        print(f"Available nodes: {len(available_nodes)}")
        
        # 获取节点负载信息
        node_loads = {}
        for node in available_nodes:
            try:
                response = requests.get(f"{self.power_controller_url}/nodes/{node}/jobs/count")
                node_loads[node] = response.json()["count"]
            except Exception as e:
                print(f"Failed to get job count for node {node}: {e}")
                return
        
        jobs_to_execute = []
        remaining_nodes = available_nodes.copy()
        
        # 遍历等待队列，尝试调度尽可能多的作业
        waiting_jobs_copy = self.waiting_jobs.copy()
        self.waiting_jobs = []
        
        for job in waiting_jobs_copy:
            if job.requested_resources <= len(remaining_nodes):
                # 按负载排序节点，优先使用负载低的节点
                sorted_nodes = sorted(remaining_nodes, 
                                    key=lambda n: (node_loads[n], n))  # 使用节点ID作为次要排序键
                
                # 选择负载最低的节点
                selected_nodes = sorted_nodes[:job.requested_resources]
                job.allocation = ProcSet(*selected_nodes)
                
                # 更新节点状态和负载
                for node in selected_nodes:
                    try:
                        requests.post(f"{self.power_controller_url}/nodes/{node}/jobs")
                        node_loads[node] += 1
                    except Exception as e:
                        print(f"Failed to add job to node {node}: {e}")
                        continue
                
                # 从可用节点列表中移除已分配的节点
                remaining_nodes = [n for n in remaining_nodes if n not in selected_nodes]
                
                jobs_to_execute.append(job)
                self.running_jobs.append(job)
                
                print(f"[Time {self.bs.time()}] Scheduled job {job.id} on nodes {selected_nodes}")
                print(f"Node loads after scheduling: {', '.join(f'node {n}: {node_loads[n]}' for n in selected_nodes)}")
            else:
                self.waiting_jobs.append(job)
                print(f"[Time {self.bs.time()}] Could not schedule job {job.id} "
                      f"(needs {job.requested_resources} nodes, only {len(remaining_nodes)} available)")
        
        # 执行作业
        if jobs_to_execute:
            print(f"[Time {self.bs.time()}] Executing {len(jobs_to_execute)} jobs")
            self.bs.execute_jobs(jobs_to_execute)
        
        # 打印节点负载分布
        if available_nodes:
            load_distribution = {}
            for load in node_loads.values():
                load_distribution[load] = load_distribution.get(load, 0) + 1
            print(f"Node load distribution: {dict(sorted(load_distribution.items()))}")
        
        print(f"After scheduling: Waiting jobs: {len(self.waiting_jobs)}, "
              f"Running jobs: {len(self.running_jobs)}\n")

    def energy_aware_schedule_jobs(self):
        """基于能效的调度算法，考虑节点运算量负载"""
        if not self.waiting_jobs:
            return
        
        try:
            # 获取可用节点
            response = requests.get(f"{self.power_controller_url}/nodes/available")
            available_nodes = response.json()["nodes"]
            
            if not available_nodes:
                print(f"[Time {self.bs.time()}] No available nodes for scheduling")
                return
                
            print(f"Available nodes: {len(available_nodes)}")
            
            # 计算节点的运算量负载
            node_loads = self._calculate_computation_loads(available_nodes)
            
            jobs_to_execute = []
            remaining_nodes = available_nodes.copy()
            
            # 遍历等待队列
            waiting_jobs_copy = self.waiting_jobs.copy()
            self.waiting_jobs = []
            
            for job in waiting_jobs_copy:
                if job.requested_resources <= len(remaining_nodes):
                    # 根据运算量负载选择最适合的节点
                    selected_nodes = self._select_nodes_by_load(
                        remaining_nodes,
                        job.requested_resources,
                        node_loads,
                        job.profile_dict['cpu']  # job需要的运算量
                    )
                    
                    if not selected_nodes:  # 如果没有合适的节点（所有节点负载都过高）
                        self.waiting_jobs.append(job)
                        print(f"[Time {self.bs.time()}] Job {job.id} delayed due to high load on all nodes")
                        continue
                    
                    job.allocation = ProcSet(*selected_nodes)
                    
                    # 更新节点负载
                    for node in selected_nodes:
                        self.power_controller.AddJobToNode(node)
                        node_loads[node] += job.profile_dict['cpu']
                    
                    remaining_nodes = [n for n in remaining_nodes if n not in selected_nodes]
                    
                    jobs_to_execute.append(job)
                    self.running_jobs.append(job)
                    
                    print(f"[Time {self.bs.time()}] Scheduled job {job.id} on nodes {selected_nodes}")
                    self._print_node_loads(selected_nodes, node_loads)
                else:
                    self.waiting_jobs.append(job)
                    print(f"[Time {self.bs.time()}] Could not schedule job {job.id} "
                          f"(needs {job.requested_resources} nodes, only {len(remaining_nodes)} available)")
            
            if jobs_to_execute:
                print(f"[Time {self.bs.time()}] Executing {len(jobs_to_execute)} jobs")
                self.bs.execute_jobs(jobs_to_execute)
            
            # 当分配作业到节点时
            for node in selected_nodes:
                try:
                    requests.post(f"{self.power_controller_url}/nodes/{node}/jobs")
                except Exception as e:
                    print(f"Failed to add job to node {node}: {e}")
                    
        except Exception as e:
            print(f"Failed to get available nodes: {e}")
        
        print(f"After scheduling: Waiting jobs: {len(self.waiting_jobs)}, "
              f"Running jobs: {len(self.running_jobs)}\n")

    def _calculate_computation_loads(self, nodes):
        """计算每个节点当前的运算量负载"""
        node_loads = {}
        NODE_CAPACITY = 56 * 40e9  # 56核 * 40GFLOPS = 2240 GFLOPS
        
        for node in nodes:
            # 获取节点上所有正在运行的作业
            running_jobs = [job for job in self.running_jobs if node in job.allocation]
            # 计算总运算量
            total_load = sum(job.profile_dict['cpu'] for job in running_jobs)
            node_loads[node] = total_load
            
        return node_loads

    def _select_nodes_by_load(self, available_nodes, required_count, node_loads, job_computation):
        """基于运算量负载选择节点
        
        策略：
        1. 计算系统中所有节点的平均负载作为参考值
        2. 设置多个负载阈值，逐步放宽节点选择条件
        3. 如果找不到满足条件的节点，逐步放宽限制
        """
        # 计算系统平均负载
        total_system_load = sum(node_loads.values())
        avg_load = total_system_load / len(node_loads) if node_loads else job_computation
        
        # 定义多个负载阈值倍数
        load_thresholds = [3, 5, 8, 12, float('inf')]
        
        selected_nodes = []
        
        # 逐步尝试不同的负载阈值
        for threshold_multiplier in load_thresholds:
            max_acceptable_load = avg_load * threshold_multiplier if avg_load > 0 else job_computation
            
            # 收集满足当前阈值的节点
            candidate_nodes = []
            for node in available_nodes:
                current_load = node_loads[node]
                if current_load + job_computation <= max_acceptable_load:
                    candidate_nodes.append((node, current_load))
            
            # 如果找到足够的节点，按策略排序并返回
            if len(candidate_nodes) >= required_count:
                # 按负载排序，优先选择已有适量负载的节点
                candidate_nodes.sort(
                    key=lambda x: (
                        0 if x[1] > 0 else 1,  # 优先选择有负载的节点
                        x[1],                   # 其次按负载大小排序
                        x[0]                    # 最后按节点ID排序
                    )
                )
                
                selected_nodes = [node for node, _ in candidate_nodes[:required_count]]
                break
        
        # 如果所有阈值都尝试过仍然找不到足够的节点，使用负载最低的节点
        if not selected_nodes and threshold_multiplier == load_thresholds[-1]:
            all_nodes = [(node, node_loads[node]) for node in available_nodes]
            all_nodes.sort(key=lambda x: (x[1], x[0]))  # 按负载和节点ID排序
            selected_nodes = [node for node, _ in all_nodes[:required_count]]
        
        return selected_nodes

    def _print_node_loads(self, nodes, node_loads):
        """打印节点负载详情"""
        # 计算系统平均负载
        avg_load = sum(node_loads.values()) / len(node_loads) if node_loads else 0
        
        print(f"System average load: {avg_load:.2e} FLOPS")
        for node in nodes:
            load = node_loads[node]
            load_ratio = load / avg_load if avg_load > 0 else 0
            print(f"Node {node} - Load: {load:.2e} FLOPS, "
                  f"Ratio to avg load: {load_ratio:.2f}")
