
class Task:
    task_id: int
    vm_type_id: int
    start_time: float
    end_time: float
    runtime: float
    requested_core: float
    requested_memory: float
    assigned: bool = False
    
    def __init__(self, task_id: int, vm_type_id: int, runtime: float, start_time: float, end_time: float, requested_core: float, requested_memory: float) -> None:
        self.task_id = task_id
        self.vm_type_id = vm_type_id
        self.start_time = start_time
        self.end_time = end_time
        self.runtime = runtime
        self.requested_core = requested_core
        self.requested_memory = requested_memory
        
        
    
    