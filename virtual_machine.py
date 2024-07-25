from vm_task import Task
from physical_machine import Machine

class VirtualMachine: 
    vm_type_id: int
    requested_core: float
    requested_memory: float
    list_of_tasks: list[Task]
    starttime: float
    endtime: float
    runtime: float
    
    def __init__(self, vm_type_id: int, requested_core: float, requested_memory: float, starttime: float, endtime: float, runtime: float) -> None:
        self.vm_type_id = vm_type_id
        self.requested_core = requested_core
        self.requested_memory = requested_memory
        self.starttime = starttime
        self.endtime = endtime
        self.runtime = runtime
        self.list_of_tasks = []
        
        
    def add_instance(self, virtual_machine):
        pass
        
    
    def remove_instance():
        pass