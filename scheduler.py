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
    
    vm_types: pd.DataFrame
    no_of_bins = math.floor(math.log(1209600, 2)) + 1
    
    def __init__(self, machine: Machine) -> None:
        self.machine = machine
        self.machine_id = machine.machine_id
        self.core_capacity = machine.cpu
        self.memory_capacity = machine.memory
        self.task_queue = []
        self.task_bins = [[] for _ in range(self.no_of_bins)]
        self.instance_bins = [[] for _ in range(self.no_of_bins)]
        self.vm_types = pd.read_csv("../outputs/vmlist2.csv")
        
    def load_tasks_to_bins(self, list_of_tasks):
        # f = open("../outputs/tasklist2.csv", 'r')
        # reader = csv.reader(f)
        # next(reader)
        # print(self.task_bins)
        for row in list_of_tasks:
            task = Task(int(row[0]), int(row[2]), float(row[5]), float(row[3]), float(row[4]))
            index = self.obtain_bin_index(self.task_bins, task.runtime)
            self.task_bins[index].append(task)
            
    def load_vms_to_bins(self, list_of_vms):
        # f = open("../outputs/assignedinstancelist2.csv", 'r')
        # reader = csv.reader(f)
        # next(reader)
         
        for row in list_of_vms:
            instance = VirtualMachine(int(row[0]), float(row[5]), float(row[6]), float(row[2]), float(row[3]), float(row[4]))
            index = self.obtain_bin_index(self.instance_bins, instance.runtime)
            self.instance_bins[index].append(instance)
            
    def obtain_bin_index(self, bins, runtime):
        # [0, 1), [1, 2), [2, 4), [4, 8), [8, 16)
        if runtime < 1:
            return 0
        
        bin_index = math.floor(math.log(runtime, 2)) + 1
        if bin_index > len(bins) - 1:
            bin_index = len(bins) - 1
        return bin_index
        
    def packer(self, list_of_tasks: list[Task], list_of_vms, task_bins: list[list[Task]], instance_bins:list[list[VirtualMachine]]):
        list_of_tasks.sort(key=lambda x: x.runtime, reverse=True)
        max_runtime_index = self.obtain_bin_index(task_bins, list_of_tasks[0].runtime)
        count = 0
        #sort the unscheduled tasks based on the runtime descending
        self.load_tasks_to_bins(list_of_tasks)
        self.load_vms_to_bins(list_of_vms)
        #iterate unscheduled tasks
        for task in task_bins[max_runtime_index]:
            #eligible instances  check if any instance with same bin is eligible to take the task
            count = 0
            for instance in instance_bins[max_runtime_index]:
                if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                    count += 1
            #if eligible instances not empty
            if count > 0:
                #assign the task to the instance with remaining runtime closest to the task with the same vmTypeId
                task_runtime = task.runtime
                for instance in instance_bins[max_runtime_index]:
                    if instance.runtime == task_runtime or math.isclose(instance.runtime, task_runtime):
                        if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                            task.assigned = True
                            instance.list_of_tasks.append(task)
                            #add task’s requested CPU and memory to memory capacity
                            self.core_capacity -= instance.requested_core
                            self.memory_capacity -= instance.requested_memory
                            break
            #else
            else:
                uppack_index = max_runtime_index
                #uppack_eligible_instances  check if any instance with greater bins is eligible
                while uppack_index < len(task_bins):
                    uppack_index += 1
                    count = 0
                    for instance in instance_bins[uppack_index]:
                        if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                            count += 1
                    #if uppack_eligible_instances not empty
                    if count > 0:
                        #assign to the instance with most available resources
                        resource_sorted_instance_list = sorted(instance_bins[uppack_index], key=lambda x: (x.requested_core, x.requested_memory), reverse=True)
                        for instance in resource_sorted_instance_list:
                            if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                task.assigned = True
                                instance.list_of_tasks.append(task)
                                #add task’s requested CPU and memory to memory capacity
                                self.core_capacity -= instance.requested_core
                                self.memory_capacity -= instance.requested_memory
                                break
                #else
                if task.assigned == False:
                    #downppack_elgible_instances  check if any lower bins instance is eligible'
                    downpack_index = max_runtime_index
                    while downpack_index >= 0:
                        downpack_index -= 1
                        count = 0
                        for instance in instance_bins[downpack_index]:
                            if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                count += 1
                        #if downpack_eligible_instances not empty
                        if count > 0:
                            promoted_index = 0
                            resource_sorted_instance_list = sorted(instance_bins[downpack_index], key=lambda x: (x.requested_core, x.requested_memory), reverse=True)
                            for index, instance in enumerate(resource_sorted_instance_list):
                                if instance.requested_core <= self.core_capacity and instance.requested_memory <= self.memory_capacity:
                                #assign to the instance with most available resources
                                    task.assigned = True
                                    instance.list_of_tasks.append(task)
                                    #add task’s requested CPU and memory to memory capacity
                                    self.core_capacity -= instance.requested_core
                                    self.memory_capacity -= instance.requested_memory
                                    promoted_index = index
                            instance_bins[downpack_index].sort(key=lambda x: (x.requested_core, x.requested_memory), reverse=True)
                            promoted_instance = instance_bins[downpack_index].pop(promoted_index)
                            instance_bins[max_runtime_index].append(promoted_instance)
                        
        
    def scaling(self, task_bins, instance_bins):
        '''
        Input Task Bins, Instance Bins, Unscheduled Tasks
        Output -
        iterate unscheduled tasks task bins descending
        while unscheduled tasks in that bin not empty
            create candidate groups
            iterate all available instance types and pair with each candidate group
            calculate the score for the pair candidate group and instance type
            acquire new instance based on the instance type and assign tasks in candidate group
        
        '''
    
    def free_tasks(self, timestamp):
        '''
        Algorithm 1: Free Tasks and Instances at Current Timestamp
        expired tasks  check if any task in the task bins has expired
        iterate expired tasks
        deduct the CPU used in the instance where the task is assigned
        deduct the memory used in the instance where the task is assigned
        drop expired tasks from the task bins
        expired instances  check if any instance in the instance bins has expired
        drop expired bins from the task bins
        iterate instance bins
        update the instance bin index according to the current timestamp\
        '''
        
    def stratus():
        pass
        
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
    
    machine = Machine(16)
    sched = Scheduler(machine)
    sched.load_tasks_to_bins()
    sched.load_vms_to_bins()
    
    
if __name__ == "__main__":
    main()