from virtual_machine import VirtualMachine
from vm_task import Task
from physical_machine import Machine

import math
import csv
import time
import pandas as pd

class Scheduler():
    
    machine: Machine
    
    core_capacity: float
    memory_capacity: float
    cpu_usage: float
    memory_usage: float
    
    task_bins: list[list[Task]]
    instance_bins: list[list[VirtualMachine]]
    
    task_queue: list[Task]
    instance_queue: list[VirtualMachine]
    
    vm_types: pd.DataFrame
    no_of_bins = math.floor(math.log(1209600, 2)) + 1
    
    def __init__(self, machine: Machine) -> None:
        self.machine = machine
        self.machine_id = machine.machine_id
        self.core_capacity = machine.cpu
        self.memory_capacity = machine.memory
        self.task_queue = []
        self.instance_queue = []
        self.task_bins = [[] for _ in range(self.no_of_bins)]
        self.instance_bins = [[] for _ in range(self.no_of_bins)]
        self.vm_types = pd.read_csv("../outputs/vmlist3.csv")
        
    def load_tasks_to_bins(self, list_of_tasks):
        for row in list_of_tasks:
            task = Task(int(row['taskId']), int(row['vmTypeId']), float(row['runtime']) ,float(row['starttime']), float(row['endtime']), float(row['requested_core']), float(row['requested_memory']))
            index = self.obtain_bin_index(self.task_bins, task.runtime)
            self.task_bins[index].append(task)
            
    def load_vms_to_bins(self, list_of_vms):
        for row in list_of_vms:
            instance = VirtualMachine(int(row['vmTypeId']), float(row['core']), float(row['memory']), float(row['starttime']), float(row['endtime']), float(row['maxruntime']))
            index = self.obtain_bin_index(self.instance_bins, instance.runtime)
            self.instance_bins[index].append(instance)
            
    def obtain_bin_index(self, bins, runtime):
        # [0, 1), [1, 2), [2, 4), [4, 8), [8, 16)
        runtime = float(runtime)
        if runtime < 1:
            return 0
        
        bin_index = math.floor(math.log(runtime, 2)) + 1
        if bin_index > len(bins) - 1:
            bin_index = len(bins) - 1
        return bin_index
        
    def packer(self, list_of_tasks, list_of_vms):
        list_of_tasks.sort(key=lambda x: float(x[5]), reverse=True)
        # max_runtime_index = self.obtain_bin_index(self.task_bins, list_of_tasks[0][5])
        count = 0
        #sort the unscheduled tasks based on the runtime descending
        self.load_tasks_to_bins(list_of_tasks)
        self.load_vms_to_bins(list_of_vms)
        
        #iterate unscheduled tasks
        for i in range(len(self.task_bins)):
            for task in self.task_bins[i]:
                #eligible instances  check if any instance with same bin is eligible to take the task
                count = 0
                for instance in self.instance_bins[i]:
                    if instance.runtime == task.runtime or math.isclose(instance.runtime, task.runtime):
                        if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                            count += 1
                #if eligible instances not empty
                if count > 0:
                    #assign the task to the instance with remaining runtime closest to the task with the same vmTypeId
                    task_runtime = task.runtime
                    for instance in self.instance_bins[i]:
                        if task.assigned == False and task.runtime <= instance.runtime and (instance.runtime == task_runtime or math.isclose(instance.runtime, task_runtime)):
                            if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                task.assigned = True
                                #add task’s requested CPU and memory to memory capacity
                                if len(instance.list_of_tasks) == 0:
                                    self.core_capacity -= instance.requested_core
                                    self.memory_capacity -= instance.requested_memory 
                                instance.list_of_tasks.append(task)
                                break
                #else
                else:
                    uppack_index = i + 1
                    #uppack_eligible_instances  check if any instance with greater bins is eligible
                    while uppack_index < len(self.task_bins):
                        count = 0
                        for instance in self.instance_bins[uppack_index]:
                            if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                count += 1
                        #if uppack_eligible_instances not empty
                        if count > 0:
                            #assign to the instance with most available resources
                            resource_sorted_instance_list = sorted(self.instance_bins[uppack_index], key=lambda x: (float(x.requested_core), float(x.requested_memory)), reverse=True)
                            for instance in resource_sorted_instance_list:
                                if task.runtime <= instance.runtime and instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity and task.assigned == False:
                                    task.assigned = True
                                    #add task’s requested CPU and memory to memory capacity
                                    if len(instance.list_of_tasks) == 0:
                                        self.core_capacity -= instance.requested_core
                                        self.memory_capacity -= instance.requested_memory
                                    instance.list_of_tasks.append(task)
                                    break
                        uppack_index += 1
                    #else
                    if task.assigned == False:
                        #downppack_elgible_instances  check if any lower bins instance is eligible'
                        downpack_index = i - 1
                        while downpack_index >= 0:
                            count = 0
                            for instance in self.instance_bins[downpack_index]:
                                if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                    count += 1
                            #if downpack_eligible_instances not empty
                            if count > 0:
                                promoted_index = 0
                                resource_sorted_instance_list = sorted(self.instance_bins[downpack_index], key=lambda x: (float(x.requested_core), float(x.requested_memory)), reverse=True)
                                for index, instance in enumerate(resource_sorted_instance_list):
                                    if task.runtime <= instance.runtime and task.assigned == False and instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                    #assign to the instance with most available resources
                                        task.assigned = True
                                        #add task’s requested CPU and memory to memory capacity
                                        if len(instance.list_of_tasks) == 0:
                                            self.core_capacity -= instance.requested_core
                                            self.memory_capacity -= instance.requested_memory
                                        instance.list_of_tasks.append(task)
                                        promoted_index = index
                                self.instance_bins[downpack_index].sort(key=lambda x: (float(x.requested_core), float(x.requested_memory)), reverse=True)
                                promoted_instance = self.instance_bins[downpack_index].pop(promoted_index)
                                self.instance_bins[i].append(promoted_instance)
                            downpack_index -= 1
        
    def scaling(self):
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
            self.instance_bins[index].append(max_resource_instance_obj)    
        
    def free_expired_tasks_and_instances(self, timestamp):
        # expired tasks  check if any task in the task bins has expired
        # iterate expired tasks
        for bin in self.instance_bins:
            for instance in bin:
                if instance.endtime <= timestamp:
                    # deduct the CPU used in the instance where the task is assigned
                    
                    added_core = instance.requested_core + self.core_capacity
                    added_memory = instance.requested_memory + self.memory_capacity
                    if added_core <= 1 and added_memory <= 1:
                        
                        self.core_capacity += instance.requested_core
                        # deduct the memory used in the instance where the task is assigned
                        self.memory_capacity += instance.requested_memory
                        
        # drop expired tasks from the task bins
        for bin in self.task_bins:
            bin[:] = [task for task in bin if task.end_time > timestamp]

        # expired instances  check if any instance in the instance bins has expired
        for bin in self.instance_bins:
            bin[:] = [instance for instance in bin if instance.endtime > timestamp]    
            bin[:] = [instance for instance in bin if len(instance.list_of_tasks) > 0]
            
        # drop expired bins from the task bins
        # iterate instance bins
        for bin in self.instance_bins:
            for index, instance in enumerate(bin):
                # update the instance bin index according to the current timestamp
                remaining_time = instance.runtime - timestamp
                current_index = self.obtain_bin_index(self.instance_bins, instance.runtime)
                new_index = self.obtain_bin_index(self.instance_bins, remaining_time)
                if current_index != new_index:
                    self.instance_bins[new_index].append(bin.pop(index))

    def stratus(self):
        fourteen_days = 1209600
        machine = Machine(16)
        
        task_csv = pd.read_csv("../outputs/tasklist2.csv")
        instance_csv = pd.read_csv("../outputs/assignedinstancelist2.csv")
        
        task_csv_pointer = 0
        instance_csv_pointer = 0
        
        for i in range(1, fourteen_days, 1000):
            print("Current timestamp is: ", i)
            self.task_queue = []
            self.instance_queue = []
            
            while task_csv_pointer < len(task_csv) and task_csv.loc[task_csv_pointer]['starttime'] < i-1 and task_csv.loc[task_csv_pointer]['starttime'] <= i:
                self.task_queue.append(task_csv.loc[task_csv_pointer])
                task_csv_pointer += 1
                    
            while instance_csv_pointer < len(instance_csv) and instance_csv.loc[instance_csv_pointer]['starttime'] < (i-1) and instance_csv.loc[instance_csv_pointer]['starttime'] <= i:
                self.instance_queue.append(instance_csv.loc[instance_csv_pointer])
                instance_csv_pointer += 1
                    
            self.packer(list_of_tasks=self.task_queue, list_of_vms=self.instance_queue)    
            self.scaling()
            self.free_expired_tasks_and_instances(i)
    
    def baseline_algo(self, list_of_tasks, list_of_vms):
        pass
    
        '''
        def log(self, scheduler, cpu_usage_list: list = None, memory_usage_list: list = None):
        while any(scheduler.keep_logging):
            if cpu_usage_list is not None:
                cpu_utilization = scheduler.current_cpu_requested / self.in_use_cpu_resource * 100 if self.in_use_cpu_resource > 0 else 0
                cpu_usage_list.append((self.env.now / 3600, self.in_use_cpu_resource, cpu_utilization))
            if memory_usage_list is not None:
                memory_utilization = scheduler.current_memory_requested / self.in_use_memory_resource * 100 if self.in_use_memory_resource > 0 else 0
                memory_usage_list.append((self.env.now / 3600, self.in_use_memory_resource, memory_utilization))
            # print(f"CPU Usage: {self.in_use_cpu_resource:.2f}")
            # print(f"Memory Usage: {self.in_use_memory_resource:.2f}")
            # print(f"Number of machines in use: {self.in_use_machine_num}")

            yield self.env.timeout(1800) # every 30 minutes
        '''
    def baseline(self):
        fourteen_days = 1209600
        machine = Machine(16)
        
        task_csv = pd.read_csv("../outputs/tasklist2.csv")
        instance_csv = pd.read_csv("../outputs/assignedinstancelist2.csv")
        
        task_csv_pointer = 0
        instance_csv_pointer = 0
        
        tasklist = []
        instancelist = []
        
        for i in range(1, fourteen_days, 1000):
            print("Current timestamp is: ", i)
            self.task_queue = []
            self.instance_queue = []
            
            while task_csv_pointer < len(task_csv) and task_csv.loc[task_csv_pointer]['starttime'] < i-1 and task_csv.loc[task_csv_pointer]['starttime'] <= i:
                self.task_queue.append(task_csv.loc[task_csv_pointer])
                task_csv_pointer += 1
                    
            while instance_csv_pointer < len(instance_csv) and instance_csv.loc[instance_csv_pointer]['starttime'] < (i-1) and instance_csv.loc[instance_csv_pointer]['starttime'] <= i:
                self.instance_queue.append(instance_csv.loc[instance_csv_pointer])
                instance_csv_pointer += 1
                    
            self.baseline_algo(list_of_tasks=self.task_queue, list_of_vms=self.instance_queue)
            
def main():
    '''
    It simulates
    instance allocation and job placement decisions made by evaluated
    schedulers (Sec. 4.3), advancing simulation time as jobs arrive and
    complete.
    
    Pending tasks are
    scheduled in batches during a periodic scheduling event; the frequency
    of the scheduling event is configurable.
    '''
    fourteen_days = 1209600
    machine = Machine(16)
    sched = Scheduler(machine)
    
    sched.stratus()
    
    
    
    
if __name__ == "__main__":
    main()