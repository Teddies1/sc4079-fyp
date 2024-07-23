from virtual_machine import VirtualMachine
from vm_task import Task
from physical_machine import Machine

import math
import csv
import time
import pandas as pd

class Scheduler():
    
    bins: list[list[VirtualMachine]]
    machine: Machine
    core_capacity: float
    memory_capacity: float
    cpu_usage: float
    memory_usage: float
    task_queue: list[Task]
    
    
    def __init__(self, machine: Machine) -> None:
        self.machine = machine
        self.machine_id = machine.machine_id
        self.core_capacity = machine.cpu
        self.memory_capacity = machine.memory
        self.task_queue = []
    
    def load_tasks_to_queue(self):
        f = open("../outputs/tasklist.csv", 'r')
        reader = csv.reader(f)
        next(reader)
        
        for row in reader:
            task = Task(int(row[0]), int(row[2]), float(row[5]), float(row[3]), float(row[4]))
            self.task_queue.append(task)
            
    def generate_bins(self):
        self.bins = [[] for _ in range(5)]
        
    def obtain_bin_index(self, bins, runtime):
        # [0, 1), [1, 2), [2, 4), [4, 8), [8, 16)
        if runtime < 1:
            return 0
        
        bin_index = math.floor(math.log(runtime, 2)) + 1
        if bin_index > len(bins) - 1:
            bin_index = len(bins) - 1
        return bin_index
    
    
    def packing(self, task_queue, bins):
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
    
    
    

def main():
    machine = Machine(16)
    sched = Scheduler(machine)
    sched.generate_bins()
    sched.load_tasks_to_queue()
    
    for row in sched.task_queue:    
        if (row.runtime >= 1):
            print(sched.obtain_bin_index(sched.bins, row.runtime))
    
    
if __name__ == "__main__":
    main()