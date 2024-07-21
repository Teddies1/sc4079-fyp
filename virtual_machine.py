from vm_task import Task
from physical_machine import Machine

class VirtualMachine: 
    machine: Machine
    vm_type_id: int
    requested_core: float
    requested_memory: float
    list_of_tasks: list[Task]
    
    def __init__(self, machine: Machine, vm_type_id: int, requested_core: float, requested_memory: float) -> None:
        self.vm_type_id = vm_type_id
        self.requested_core = requested_core
        self.requested_memory = requested_memory
        self.list_of_tasks = []
        
       
    def add_instance(self, virtual_machine):
        pass
        
    
    def remove_instance():
        pass