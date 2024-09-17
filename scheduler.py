from virtual_machine import VirtualMachine
from vm_task import Task
from instance import Instance

import math
import csv
import time
import pandas as pd

class Scheduler():
    
    machine: Instance
    
    core_capacity: float
    memory_capacity: float
    cpu_usage: float
    memory_usage: float
    
    task_bins: list[list[Task]]
    instance_bins: list[list[Instance]]
    
    task_queue: list[Task]
    instance_pool: list[Instance]
    
    stratus_core_log: list[float]
    stratus_memory_log: list[float]
    
    baseline_core_log: list[float]
    baseline_memory_log: list[float]
    
    vm_types: pd.DataFrame
    no_of_bins = math.floor(math.log(1209600, 2)) + 1
    instance_pool_size = 35
    
    def __init__(self) -> None:
        self.task_queue = []
        self.task_bins = [[] for _ in range(self.no_of_bins)]
        
        self.instance_bins = [[] for _ in range(self.no_of_bins)]
        self.instance_pool = [Instance(i) for i in range(self.instance_pool_size)]
        
        self.vm_types = pd.read_csv("../outputs/vmlist3.csv")
        
    def obtain_bin_index(self, bins, runtime) -> int:
        runtime = float(runtime)
        if runtime < 1:
            return 0
        
        bin_index = math.floor(math.log(runtime, 2)) + 1
        if bin_index > len(bins) - 1:
            bin_index = len(bins) - 1
        return bin_index
    
    def load_tasks_to_bins(self, list_of_tasks, algo) -> None:
        for row in list_of_tasks:
            task = Task(int(row['taskId']), int(row['vmTypeId']), float(row['runtime']) ,float(row['starttime']), float(row['endtime']), float(row['requested_core']), float(row['requested_memory']))
            if algo == "stratus":
                index = self.obtain_bin_index(self.task_bins, task.runtime)
            elif algo == "baseline":
                index = 0
            self.task_bins[index].append(task)
            
    def load_instances_to_bins(self, list_of_vms: list[Instance], algo) -> None:
        for instance in list_of_vms:
            instance.max_runtime = instance.get_max_runtime()
            if algo == "stratus":
                index = self.obtain_bin_index(self.instance_bins, instance.max_runtime)
            elif algo == "baseline":
                index = 0
            self.instance_bins[index].append(instance)
            
    def update_instance_bins(self, algo):
        self.instance_bins = [[] for _ in range(self.no_of_bins)]
        for instance in self.instance_pool:
            instance.max_runtime = instance.get_max_runtime()
            if algo == "stratus":
                runtime_bin_index = self.obtain_bin_index(self.instance_bins, instance.max_runtime)
            elif algo == "baseline":
                runtime_bin_index = 0
            # runtime_bin_index = instance.machine_id % len(self.instance_bins)
            self.instance_bins[runtime_bin_index].append(instance)
    
    def packer(self, list_of_tasks) -> None:
        list_of_tasks.sort(key=lambda x: float(x[5]), reverse=True)
        count = 0
        #sort the unscheduled tasks based on the runtime descending
        self.load_tasks_to_bins(list_of_tasks, "stratus")
        self.update_instance_bins("stratus")
        
        # for bin in self.instance_bins:
        #     print(bin)
                
        #iterate unscheduled tasks
        for i in range(len(self.task_bins)):
            for task in self.task_bins[i]:
                #eligible instances  check if any instance with same bin is eligible to take the task
                count = 0
                for instance in self.instance_bins[i]:
                    instance.max_runtime = instance.get_max_runtime()
                    # TODO: change close runtime logic
                    if instance.max_runtime == task.runtime or math.isclose(instance.max_runtime, task.runtime):
                        if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                            count += 1
                print("number of eligible instances:", count)
                #if eligible instances not empty
                if count > 0:
                    #assign the task to the instance with remaining runtime closest to the task with the same vmTypeId
                    for instance in self.instance_bins[i]:
                        # TODO: change close runtime logic
                        if task.assigned == False and task.runtime <= instance.max_runtime and (instance.max_runtime == task.runtime or math.isclose(instance.max_runtime, task.runtime)):
                            if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                task.assigned = True
                                #add task’s requested CPU and memory to memory capacity
                                print("Allocated task to instance in packing phase")
                                chosen_machine_id = instance.machine_id
                                chosen_instance = self.instance_pool[chosen_machine_id]
                                
                                chosen_instance.core_capacity -= task.requested_core
                                chosen_instance.memory_capacity -= task.requested_memory
                                
                                chosen_instance.list_of_tasks.append(task)
                                chosen_instance.max_runtime = chosen_instance.get_max_runtime() 
                                break
                #else
                else:
                    print("entering uppacking")
                    uppack_index = i + 1
                    #uppack_eligible_instances  check if any instance with greater bins is eligible
                    while uppack_index < len(self.task_bins):
                        count = 0
                        for instance in self.instance_bins[uppack_index]:
                            if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                count += 1
                        # print("uppack count: ", count)
                        #if uppack_eligible_instances not empty
                        if count > 0:
                            #assign to the instance with most available resources
                            resource_sorted_instance_list = sorted(self.instance_bins[uppack_index], key=lambda x: (float(x.core_capacity), float(x.memory_capacity)), reverse=True)
                            for instance in resource_sorted_instance_list:
                                if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity and task.assigned == False:
                                    task.assigned = True
                                    #add task’s requested CPU and memory to memory capacity
                                    print("Allocated task to instance in uppacking phase")
                                    chosen_machine_id = instance.machine_id
                                    chosen_instance = self.instance_pool[chosen_machine_id]
                                    
                                    chosen_instance.core_capacity -= task.requested_core
                                    chosen_instance.memory_capacity -= task.requested_memory
                                    
                                    chosen_instance.list_of_tasks.append(task)
                                    chosen_instance.max_runtime = chosen_instance.get_max_runtime()
                                    break
                        uppack_index += 1
                    #else
                    if task.assigned == False:
                        print("entering downpacking")
                        #downppack_elgible_instances  check if any lower bins instance is eligible'
                        downpack_index = i - 1
                        while downpack_index >= 0:
                            count = 0
                            for instance in self.instance_bins[downpack_index]:
                                if task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                    count += 1
                            #if downpack_eligible_instances not empty
                            # print("downpack count: ", count)
                            if count > 0:
                                promoted_index = 0
                                resource_sorted_instance_list = sorted(self.instance_bins[downpack_index], key=lambda x: (float(x.core_capacity), float(x.memory_capacity)), reverse=True)
                                for index, instance in enumerate(resource_sorted_instance_list):
                                    if task.assigned == False and task.requested_core <= instance.core_capacity and task.requested_memory <= instance.memory_capacity:
                                    #assign to the instance with most available resources
                                        task.assigned = True
                                        #add task’s requested CPU and memory to memory capacity
                                        print("Allocated task to instance in downpacking phase")
                                        chosen_machine_id = instance.machine_id
                                        chosen_instance = self.instance_pool[chosen_machine_id]
                                        
                                        chosen_instance.core_capacity -= task.requested_core
                                        chosen_instance.memory_capacity -= task.requested_memory
                                        
                                        chosen_instance.list_of_tasks.append(task)
                                        chosen_instance.max_runtime = chosen_instance.get_max_runtime()
                                #         promoted_index = index
                                # self.instance_bins[downpack_index].sort(key=lambda x: (float(x.core_capacity), float(x.memory_capacity)), reverse=True)
                                # promoted_instance = self.instance_bins[downpack_index].pop(promoted_index)
                                # self.instance_bins[i].append(promoted_instance)
                            downpack_index -= 1
        for bin in self.instance_bins:
            for instance in bin:
                print(len(instance.list_of_tasks))
                
    def scaling(self) -> None:
        eligible_vm_ids = []
        sorted_vm_list = self.vm_types.sort_values(["core", "memory"], ascending=False)

        for i, vm in sorted_vm_list.iterrows():
            flag = 0
            for bin in self.instance_bins:
                for instance in bin:
                    if vm["vmTypeId"] == instance.vm_type_id:
                        flag = 1
            if flag == 0 and self.core_capacity >= vm["core"] and self.memory_capacity >= vm["memory"]:
                eligible_vm_ids.append(vm)
                
        if len(eligible_vm_ids) > 0:
            max_resource_vm = eligible_vm_ids[0]
            max_resource_instance_obj = VirtualMachine(int(max_resource_vm["vmTypeId"]), float(max_resource_vm["core"]), float(max_resource_vm["memory"]), float(max_resource_vm["starttime"]), float(max_resource_vm["endtime"]), float(max_resource_vm["maxruntime"]))
            
            for i in range(len(self.task_bins)-1 , -1, -1):
                for task in self.task_bins[i]:
                    if task.assigned == False:
                        max_resource_instance_obj.list_of_tasks.append(task)
                        
            index = self.obtain_bin_index(self.instance_bins, max_resource_instance_obj.runtime)
            self.core_capacity -= max_resource_instance_obj.requested_core
            self.memory_capacity -= max_resource_instance_obj.requested_memory
            self.instance_bins[index].append(max_resource_instance_obj)    
        
    def free_expired_tasks_and_instances_stratus(self, timestamp) -> None:
        for bin in self.instance_bins:
            for instance in bin:
                if instance.endtime <= timestamp or len(instance.list_of_tasks) == 0:
                    added_core = instance.requested_core + self.core_capacity
                    added_memory = instance.requested_memory + self.memory_capacity
                    if added_core <= 1 and added_memory <= 1:
                        self.core_capacity += instance.requested_core
                        self.memory_capacity += instance.requested_memory
                        
        for bin in self.task_bins:
            bin[:] = [task for task in bin if task.end_time > timestamp]
            bin[:] = [task for task in bin if task.assigned == False]

        for bin in self.instance_bins:
            bin[:] = [instance for instance in bin if instance.endtime > timestamp]    
            bin[:] = [instance for instance in bin if len(instance.list_of_tasks) > 0]
            
        for bin in self.instance_bins:
            for index, instance in enumerate(bin):
                remaining_time = instance.runtime - timestamp
                current_index = self.obtain_bin_index(self.instance_bins, instance.runtime)
                new_index = self.obtain_bin_index(self.instance_bins, remaining_time)
                if current_index != new_index:
                    self.instance_bins[new_index].append(bin.pop(index))
                    
    def free_expired_tasks_and_instances_baseline(self, timestamp) -> None:
        for instance in self.instance_bins[0]:
            if instance.endtime <= timestamp or len(instance.list_of_tasks) == 0:
                added_core = instance.requested_core + self.core_capacity
                added_memory = instance.requested_memory + self.memory_capacity
                if added_core <= 1 and added_memory <= 1:
                    self.core_capacity += instance.requested_core
                    self.memory_capacity += instance.requested_memory
                    
        self.instance_bins[0][:] = [instance for instance in self.instance_bins[0] if instance.endtime > timestamp]    
        self.instance_bins[0][:] = [instance for instance in self.instance_bins[0] if len(instance.list_of_tasks) > 0]
        
        self.task_bins[0][:] = [task for task in self.task_bins[0] if task.end_time > timestamp]
        self.task_bins[0][:] = [task for task in self.task_bins[0] if task.assigned == False]
        
    def stratus(self, total_time, interval) -> None:
        self.memory_capacity = 1
        self.core_capacity = 1
        
        task_csv = pd.read_csv("../outputs/tasklist2.csv")
        instance_csv = pd.read_csv("../outputs/assignedinstancelist2.csv")
        
        task_csv_pointer = 0
        instance_csv_pointer = 0
        
        self.stratus_core_log = []
        self.stratus_memory_log = []
        
        for i in range(1, total_time, interval):
            print("Current timestamp is: ", i)
            self.task_queue = []
            self.instance_queue = []
            
            while task_csv_pointer < len(task_csv) and task_csv.loc[task_csv_pointer]['starttime'] < i-1 and task_csv.loc[task_csv_pointer]['starttime'] <= i:
                self.task_queue.append(task_csv.loc[task_csv_pointer])
                task_csv_pointer += 1
                    
            while instance_csv_pointer < len(instance_csv) and instance_csv.loc[instance_csv_pointer]['starttime'] < (i-1) and instance_csv.loc[instance_csv_pointer]['starttime'] <= i:
                self.instance_queue.append(instance_csv.loc[instance_csv_pointer])
                instance_csv_pointer += 1
                    
            self.packer(list_of_tasks=self.task_queue)    
            # self.scaling()
            
            # self.stratus_core_log.append(1 - self.core_capacity)
            # self.stratus_memory_log.append(1 - self.memory_capacity)
            
            # self.free_expired_tasks_and_instances_stratus(i)
        
    def baseline_algo(self, list_of_tasks) -> None:
        self.load_tasks_to_bins(list_of_tasks, "baseline")
        self.update_instance_bins("baseline")
        
        for task in self.task_bins[0]:
            if task.assigned == False:
                for instance in self.instance_bins[0]:
                    if instance.runtime >= task.runtime and len(instance.list_of_tasks) == 0 and instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                        task.assigned = True
                        #add task’s requested CPU and memory to memory capacity      
                        if len(instance.list_of_tasks) == 0:  
                            self.core_capacity -= instance.requested_core
                            self.memory_capacity -= instance.requested_memory 
                        instance.list_of_tasks.append(task)
                        break
                                                    
    def baseline(self, total_time, interval) -> None:
        self.memory_capacity = 1
        self.core_capacity = 1
        
        self.instance_bins = [[]]
        self.task_bins = [[]]
        
        task_csv = pd.read_csv("../outputs/tasklist2.csv")
        instance_csv = pd.read_csv("../outputs/assignedinstancelist2.csv")
        
        task_csv_pointer = 0
        instance_csv_pointer = 0
        
        self.baseline_core_log = []
        self.baseline_memory_log = []
        
        for i in range(1, total_time, interval):
            print("Current timestamp is: ", i)
            self.task_queue = []
            self.instance_queue = []
            
            while task_csv_pointer < len(task_csv) and task_csv.loc[task_csv_pointer]['starttime'] < i-1 and task_csv.loc[task_csv_pointer]['starttime'] <= i:
                self.task_queue.append(task_csv.loc[task_csv_pointer])
                task_csv_pointer += 1
                    
            while instance_csv_pointer < len(instance_csv) and instance_csv.loc[instance_csv_pointer]['starttime'] < (i-1) and instance_csv.loc[instance_csv_pointer]['starttime'] <= i:
                self.instance_queue.append(instance_csv.loc[instance_csv_pointer])
                instance_csv_pointer += 1
                
            self.baseline_algo(list_of_tasks=self.task_queue)
            
            self.baseline_core_log.append(1 - self.core_capacity)
            self.baseline_memory_log.append(1 - self.memory_capacity)
            
            self.free_expired_tasks_and_instances_baseline(i)
        
def main() -> None:
    fourteen_days = 1209600
    test_duration = 10000
    interval = 1000
    machine = Instance(16)
    sched = Scheduler()
    
    print("-----Running Stratus Algo-----")
    sched.stratus(test_duration, interval)    
    print("-----Finished Stratus Algo-----")
    
    # print("-----Running Baseline Algo-----")
    # sched.baseline(fourteen_days, interval)
    # print("-----Finished Baseline Algo-----")
    
    # timestamp_array = [i for i in range(1, fourteen_days, interval)]
    
    # print("----Writing to CSV-----")
    # with open(f"../logging/core_usage.csv", "w") as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["timestamp", "baseline_core_util", "stratus_core_util"])
    #     writer.writerows(zip(timestamp_array, sched.baseline_core_log, sched.stratus_core_log))
    
    # with open(f"../logging/memory_usage.csv", "w") as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["timestamp", "baseline_memory_util", "stratus_memory_util"])
    #     writer.writerows(zip(timestamp_array, sched.baseline_memory_log, sched.stratus_memory_log))
    # print("----Finished Writing to CSV-----")    
if __name__ == "__main__":
    main()