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
    vm_types: pd.DataFrame
    
    def __init__(self, machine: Machine) -> None:
        self.machine = machine
        self.machine_id = machine.machine_id
        self.core_capacity = machine.cpu
        self.memory_capacity = machine.memory
        self.task_queue = []
        self.task_bins = [[] for _ in range(5)]
        self.instance_bins = [[] for _ in range(5)]
        self.vm_types = pd.read_csv("../outputs/vmlist.csv")
        
    def load_tasks_to_bins(self):
        f = open("../outputs/tasklist.csv", 'r')
        reader = csv.reader(f)
        next(reader)
        print(self.task_bins)
        for row in reader:
            task = Task(int(row[0]), int(row[2]), float(row[5]), float(row[3]), float(row[4]))
            index = self.obtain_bin_index(self.task_bins, task.runtime)
            self.task_bins[index].append(task)
            
    def load_vms_to_bins(self):
        f = open("../outputs/assignedinstancelist.csv", 'r')
        reader = csv.reader(f)
        next(reader)
        '''
        Similarly,
        an instance is assigned to a bin according to the remaining
        runtime of the instance, which is the longest remaining runtime of
        the tasks assigned to the instance.
        '''
        for row in reader:
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
        
    def packer(self, task_queue, bins):
        '''
        sort the unscheduled tasks based on the runtime descending
        
        iterate unscheduled tasks
        eligible instances  check if any instance with same bin is eligible to take the task
        if eligible instances not empty
            assign the task to the instance with remaining runtime closest to the task with the same vmTypeId
            add task’s requested CPU and memory to memory capacity
        else
            uppack_eligible_instances  check if any instance with greater bins is eligible
            
        if uppack_eligible_instances not empty
            assign to the instance with most available resources
            add task’s requested CPU and memory to memory capacity
        else
            downppack_elgible_instances  check if any lower bins instance is eligible
        if downpack_eligible_instances not empty
            assign to the instance with most available resources
            add task’s requested CPU and memory to memory capacity
        '''
        
    def scaling(self, task_queue, bins):
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
    
    def schedule(self, timestamp):
        '''
        Algorithm 1: Free Tasks and Instances at Current Timestamp
        Input Current Timestamp
        Output -
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
    
        '''
        Algorithm description. At the beginning of a scheduling
        event, the packer organizes tasks and instances into their appropriate
        bins. Tasks are then considered for placement in descending
        order by runtime—longest task first. For each task, the Packer
        attempts to assign it to an available instance in two phases: the
        up-packing phase and the down-packing phase.
        '''
    
def main():
    '''
    It simulates
    instance allocation and job placement decisions made by evaluated
    schedulers (Sec. 4.3), advancing simulation time as jobs arrive and
    complete.
    '''
    machine = Machine(16)
    sched = Scheduler(machine)
    sched.load_tasks_to_bins()
    sched.load_vms_to_bins()
    
    # for bin in sched.task_bins:
    #     print(len(bin))
        
    # for bin in sched.instance_bins:
    #     print(len(bin))

    print(sched.vm_types)    
    
if __name__ == "__main__":
    main()