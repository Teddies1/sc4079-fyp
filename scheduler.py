from vm_task import Task
from instance import Instance

import math
import time
import pandas as pd

class Scheduler():    
    task_bins: list[list[Task]]
    instance_bins: list[list[Instance]]
    
    task_queue: list[Task]
    instance_pool: list[Instance]
    
    stratus_logging_list: list
    baseline_logging_list: list
    
    no_of_bins = math.floor(math.log(1209600, 2)) + 1
    instance_pool_size = 35
    unique_id_pointer = 35
    
    def __init__(self) -> None:
        self.task_queue = []
        self.task_bins = [[] for _ in range(self.no_of_bins)]
        self.instance_bins = [[] for _ in range(self.no_of_bins)]
        self.instance_pool = [Instance(i, i) for i in range(self.instance_pool_size)]
                
    def obtain_bin_index(self, bins, runtime) -> int:
        runtime = float(runtime)
        if runtime < 1:
            return 0
        
        bin_index = math.floor(math.log(runtime, 2)) + 1
        if bin_index > len(bins) - 1:
            bin_index = len(bins) - 1
        return bin_index
    
    def load_tasks_to_bins(self, list_of_tasks, algo) -> None:
        print("loading tasks to bins")
        self.task_bins = [[] for _ in range(self.no_of_bins)]
        
        for row in list_of_tasks:
            task = Task(int(row['taskId']), int(row['vmTypeId']), float(row['runtime']) ,float(row['starttime']), float(row['endtime']), float(row['requested_core']), float(row['requested_memory']))
            if algo == "stratus":
                index = self.obtain_bin_index(self.task_bins, task.runtime)
            elif algo == "baseline":
                index = 0
            self.task_bins[index].append(task)
            
    def update_instance_bins(self, algo):
        print("updating instance bins")
        self.instance_bins = [[] for _ in range(self.no_of_bins)]
        
        for instance in self.instance_pool:
            instance.max_runtime = instance.get_max_runtime()
            if algo == "stratus":
                runtime_bin_index = self.obtain_bin_index(self.instance_bins, instance.max_runtime)
                self.instance_bins[runtime_bin_index].append(instance)
            elif algo == "baseline": 
                runtime_bin_index = 0
                if len(instance.list_of_tasks) == 0:
                    self.instance_bins[runtime_bin_index].append(instance)
                    
    def free_expired_tasks_and_instances_stratus(self, timestamp) -> None:
        print("free stratus")

        for instance in self.instance_pool:
            instance.list_of_tasks.sort(key=lambda x: x.end_time)
            for task in instance.list_of_tasks:
                if task.end_time <= timestamp:
                    if (instance.core_capacity + task.requested_core <= 1) and (instance.memory_capacity + task.requested_memory <= 1):
                        instance.core_capacity += task.requested_core
                        instance.memory_capacity += task.requested_memory
            instance.list_of_tasks[:] = [task for task in instance.list_of_tasks if task.end_time > timestamp]
            
        self.instance_pool[:] = [instance for instance in self.instance_pool if len(instance.list_of_tasks) > 0]
            
    def free_expired_tasks_and_instances_baseline(self, timestamp) -> None:
        print("free baseline") 
        for instance in self.instance_pool:
            instance.list_of_tasks[:] = [task for task in instance.list_of_tasks if task.end_time > timestamp]
        
        self.instance_pool[:] = [instance for instance in self.instance_pool if len(instance.list_of_tasks) > 0]
        
    def packer(self, list_of_tasks) -> None:
        print("packer")
        count = 0
        self.load_tasks_to_bins(list_of_tasks, "stratus")      
        self.update_instance_bins("stratus")
        
        for i in range(len(self.task_bins)):
            self.task_bins[i].sort(key=lambda x: x.runtime, reverse=True)
            for task in self.task_bins[i]:
                count = 0
                for instance in self.instance_bins[i]:
                    instance.max_runtime = instance.get_max_runtime()
                    if instance.max_runtime == task.runtime or math.isclose(instance.max_runtime, task.runtime):
                        if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                            count += 1
                if count > 0:
                    for instance in self.instance_bins[i]:
                        if task.assigned == False and task.runtime <= instance.max_runtime and (instance.max_runtime == task.runtime or math.isclose(instance.max_runtime, task.runtime)):
                            if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                task.assigned = True
                                
                                chosen_machine_id = instance.unique_id
                                chosen_instance = next((x for x in self.instance_pool if x.unique_id == chosen_machine_id))
                                
                                chosen_instance.core_capacity -= task.requested_core
                                chosen_instance.memory_capacity -= task.requested_memory
                                
                                chosen_instance.list_of_tasks.append(task)
                                chosen_instance.max_runtime = chosen_instance.get_max_runtime() 
                                break
                #else
                else:
                    uppack_index = i + 1
                    while uppack_index < len(self.task_bins):
                        count = 0
                        for instance in self.instance_bins[uppack_index]:
                            if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                count += 1
                        if count > 0:
                            resource_sorted_instance_list = sorted(self.instance_bins[uppack_index], key=lambda x: (float(x.core_capacity), float(x.memory_capacity)), reverse=True)
                            for instance in resource_sorted_instance_list:
                                if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity and task.assigned == False:
                                    task.assigned = True
                                    
                                    chosen_machine_id = instance.unique_id
                                    chosen_instance = next((x for x in self.instance_pool if x.unique_id == chosen_machine_id))
                                    
                                    chosen_instance.core_capacity -= task.requested_core
                                    chosen_instance.memory_capacity -= task.requested_memory
                                    
                                    chosen_instance.list_of_tasks.append(task)
                                    chosen_instance.max_runtime = chosen_instance.get_max_runtime()
                                    break
                        uppack_index += 1
                    if task.assigned == False:
                        downpack_index = i - 1
                        while downpack_index >= 0:
                            count = 0
                            for instance in self.instance_bins[downpack_index]:
                                if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                    count += 1
                            if count > 0:
                                resource_sorted_instance_list = sorted(self.instance_bins[downpack_index], key=lambda x: (float(x.core_capacity), float(x.memory_capacity)), reverse=True)
                                for instance in (resource_sorted_instance_list):
                                    if task.assigned == False and task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                        task.assigned = True
                                        
                                        chosen_machine_id = instance.unique_id
                                        chosen_instance = next((x for x in self.instance_pool if x.unique_id == chosen_machine_id))
                                        
                                        chosen_instance.core_capacity -= task.requested_core
                                        chosen_instance.memory_capacity -= task.requested_memory
                                        
                                        chosen_instance.list_of_tasks.append(task)
                                        chosen_instance.max_runtime = chosen_instance.get_max_runtime()
                            downpack_index -= 1
                            
    def scaling(self) -> None:
        print("scaler")
        candidate_group_list: list[list[Task]] = []
        unassigned_task_list: list[Task] = []
        
        max_runtime_instance = max(self.instance_pool, key=lambda x: x.max_runtime)
        scaler_max_runtime = max_runtime_instance.max_runtime
        min_runtime_instance = min(self.instance_pool, key=lambda x: x.max_runtime)
        scaler_min_runtime = min_runtime_instance.max_runtime
        
        for i in range(len(self.task_bins)-1, -1, -1):
            for task in self.task_bins[i]:
                if task.assigned == False:
                    unassigned_task_list.append(task)
                    
        unassigned_task_list.sort(key=lambda x: x.runtime, reverse=True)
        candidate_group_flag = 0
        candidate_group_size = 1
        while len(unassigned_task_list) > 0:
            candidate_group_flag = 0
            while candidate_group_flag == 0:
                unassigned_task_list_pointer = 0
                cumulative_task_memory = 0
                cumulative_task_cpu = 0
                new_candidate_group = []
                while unassigned_task_list_pointer < len(unassigned_task_list) and len(new_candidate_group) < candidate_group_size:
                    new_unassigned_task = unassigned_task_list[unassigned_task_list_pointer]
                    unassigned_task_list_pointer += 1
                    
                    new_candidate_group.append(new_unassigned_task)
                    cumulative_task_memory += task.requested_memory
                    cumulative_task_cpu += task.requested_core
                if cumulative_task_cpu <= 1 and cumulative_task_memory <= 1:
                    candidate_group_list.append([new_candidate_group, cumulative_task_cpu, cumulative_task_memory])
                    candidate_group_size += 1
                else:
                    candidate_group_flag = 1
                if unassigned_task_list_pointer >= len(unassigned_task_list):
                    break 
            max_efficiency_score = -1
            max_candidate_instance_group = ()
            max_cumulative_cpu = 0
            max_cumulative_memory = 0
            
            for group in candidate_group_list:
                for instance in self.instance_pool:
                    score = 0
                    candidate_instance_group = (group[0], instance.machine_id)
                    cpu_ratio = group[1]
                    memory_ratio = group[2]
                    normalised_constraining_resouce = max(cpu_ratio, memory_ratio)
                    normalised_runtime = (instance.max_runtime - scaler_min_runtime)/ (scaler_max_runtime - scaler_min_runtime)
                    if normalised_runtime > 0:
                        score = normalised_constraining_resouce / normalised_runtime
                    else:
                        score = 0
                    if score > max_efficiency_score:
                        max_efficiency_score = score
                        max_candidate_instance_group = candidate_instance_group
                        max_cumulative_cpu = group[1]
                        max_cumulative_memory = group[2]
                        
            candidate_group_length = len(max_candidate_instance_group[0])
            del unassigned_task_list[:candidate_group_length]
            
            if candidate_group_length > 0:
                max_candidate_group = max_candidate_instance_group[0]
                max_instance_id = max_candidate_instance_group[1]
                new_max_instance = Instance(self.unique_id_pointer, max_instance_id)
                self.unique_id_pointer += 1
                
                new_max_instance.list_of_tasks += max_candidate_group
                new_max_instance.max_runtime = new_max_instance.get_max_runtime()
                new_max_instance.core_capacity -= max_cumulative_cpu
                new_max_instance.memory_capacity -= max_cumulative_memory
                self.instance_pool.append(new_max_instance)
            
    def stratus(self, total_time, interval) -> None:
        print("stratus")
        self.unique_id_pointer = 35
        
        task_csv = pd.read_csv("../outputs/tasklist2.csv")        
        task_csv_pointer = 0

        self.stratus_logging_list = []
        
        for i in range(1, total_time, interval):
            print("Current Stratus timestamp is: ", i)
            self.task_queue = []
            
            while task_csv_pointer < len(task_csv) and task_csv.loc[task_csv_pointer]['starttime'] <= i:
                self.task_queue.append(task_csv.loc[task_csv_pointer])
                task_csv_pointer += 1
                    
            self.packer(list_of_tasks=self.task_queue)    
            self.scaling()          
            
            instance_pool_length = len(self.instance_pool)
            core_sum = sum((1-instance.core_capacity) for instance in self.instance_pool)
            mem_sum = sum((1-instance.memory_capacity) for instance in self.instance_pool)
            runtime_sum = sum(instance.max_runtime for instance in self.instance_pool)
            
            self.stratus_logging_list.append({
                "timestamp": i,
                "avg_stratus_runtime": runtime_sum / instance_pool_length,
                "avg_stratus_memory_util": mem_sum / instance_pool_length,
                "avg_stratus_core_util": core_sum / instance_pool_length,
                "num_instances_stratus": instance_pool_length
            })
            
            self.free_expired_tasks_and_instances_stratus(i)
            
    def baseline_algo(self, list_of_tasks) -> None:
        print("baseline algorithm")
        self.load_tasks_to_bins(list_of_tasks, "baseline")
        
        for task in self.task_bins[0]:
            if task.assigned == False:
                new_instance = Instance(self.unique_id_pointer, self.unique_id_pointer % 35)
                self.unique_id_pointer += 1
                new_instance.list_of_tasks.append(task)
                new_instance.max_runtime = new_instance.get_max_runtime()
                new_instance.core_capacity -= task.requested_core
                new_instance.memory_capacity -= task.requested_memory
                
                self.instance_pool.append(new_instance)     
                
    def baseline(self, total_time, interval) -> None:
        print("baseline")
        self.unique_id_pointer = 0
        
        self.instance_pool = []
        self.instance_bins = [[]]
        self.task_bins = [[]]
        
        task_csv = pd.read_csv("../outputs/tasklist2.csv")        
        task_csv_pointer = 0

        self.baseline_logging_list = []
        
        for i in range(1, total_time, interval):
            print("Current baseline timestamp is: ", i)
            self.task_queue = []
            
            while task_csv_pointer < len(task_csv) and task_csv.loc[task_csv_pointer]['starttime'] <= i:
                self.task_queue.append(task_csv.loc[task_csv_pointer])
                task_csv_pointer += 1
                
            self.baseline_algo(list_of_tasks=self.task_queue)

            instance_pool_length = len(self.instance_pool)
            core_sum = sum((1-instance.core_capacity) for instance in self.instance_pool)
            mem_sum = sum((1-instance.memory_capacity) for instance in self.instance_pool)
            runtime_sum = sum(instance.max_runtime for instance in self.instance_pool)
            
            self.baseline_logging_list.append({
                "timestamp": i,
                "avg_baseline_runtime": runtime_sum / instance_pool_length,
                "avg_baseline_memory_util": mem_sum / instance_pool_length,
                "avg_baseline_core_util": core_sum / instance_pool_length,
                "num_instances_baseline": instance_pool_length
            })
            
            self.free_expired_tasks_and_instances_baseline(i)
            
def main() -> None:
    fourteen_days = 1209600
    test_duration = 20000
    interval = 1000
    sched = Scheduler()
    
    
    print("-----Running Stratus Algo-----")
    tic1 = time.perf_counter()
    sched.stratus(fourteen_days, interval)
    toc1 = time.perf_counter()  
    print("-----Finished Stratus Algo-----")
    print("-----Running Baseline Algo-----")
    tic2 = time.perf_counter()
    sched.baseline(fourteen_days, interval)
    toc2 = time.perf_counter()
    print("-----Finished Baseline Algo-----")
    print(f"-----Finished Stratus Algo in: {toc1 - tic1:0.4f} seconds -----")
    print(f"-----Finished Baseline Algo in: {toc2 - tic2:0.4f} seconds -----")
        
    print("-----Writing to CSV-----")  
    df_stratus = pd.DataFrame(sched.stratus_logging_list)
    df_baseline = pd.DataFrame(sched.baseline_logging_list)
    
    df_stratus.to_csv("../logging/stratus_output.csv", index=False)
    df_baseline.to_csv("../logging/baseline_output.csv", index=False)
    print("-----Finished Writing to CSV-----")  
      
if __name__ == "__main__":
    main()