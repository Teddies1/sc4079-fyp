from virtual_machine import VirtualMachine
from vm_task import Task

class Scheduler():
    
    bins: list[list[VirtualMachine]]
    machine_id: int
    core_capacity: float
    memory_capacity: float
    
    def __init__(self, machine_id: int=16, core_capacity: float=1, memory_capacity: float=1) -> None:
        self.machine_id = machine_id
        self.core_capacity = core_capacity
        self.memory_capacity = memory_capacity
        
        
    def up_packing():
        pass
    
    
    def down_packing():
        pass
    
    
    

    