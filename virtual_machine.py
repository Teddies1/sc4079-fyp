from vm_task import Task


class VirtualMachine:
    vm_type_id: int
    requested_core: float
    requested_memory: float
    list_of_tasks: list[Task]
    
    def __init__(self, vm_type_id: int, requested_core: float, requested_memory: float, list_of_tasks: list[Task]) -> None:
        self.vm_type_id = vm_type_id
        self.requested_core = requested_core
        self.requested_memory = requested_memory
        self.list_of_tasks = list_of_tasks
        
        
    
        