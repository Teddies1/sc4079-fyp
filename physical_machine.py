from virtual_machine import VirtualMachine
from vm_task import Task

class Machine(object):
    machine_id: int
    core_capacity: float
    memory_capacity: float
    queue: list
    bins: list[list[VirtualMachine]]
    
    def __init__(self, bins: list[list[VirtualMachine]], queue: list, machine_id: int=16, core_capacity: float=1, memory_capacity: float=1) -> None:
        self.machine_id = machine_id
        self.core_capacity = core_capacity
        self.memory_capacity = memory_capacity
        self.queue = queue
        self.bins = bins
        
    def add_instance(self, virtual_machine):
        pass
        
    
    def remove_instance():
        pass
    
    