from virtual_machine import VirtualMachine
from vm_task import Task
from physical_machine import Machine

class Scheduler():
    
    bins: list[list[VirtualMachine]]
    machine: Machine
    core_capacity: float
    memory_capacity: float
    cpu_usage: float
    memory_usage: float
    task_queue: list[Task]
    
    def __init__(self, machine: Machine, machine_id: int=16) -> None:
        self.machine_id = machine_id
        self.core_capacity = machine.cpu
        self.memory_capacity = machine.memory
        self.task_queue = []
        self.machine = machine
        
    def up_packing():
        pass
    
    
    def down_packing():
        pass
    
    
    

    