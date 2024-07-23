class Task:
    task_id: int
    vm_type_id: int
    start_time: float
    end_time: float
    runtime: float
    running: bool = False
    
    def __init__(self, task_id: int, runtime, float, vm_type_id: int, start_time: float, end_time: float) -> None:
        self.task_id = task_id
        self.vm_type_id = vm_type_id
        self.start_time = start_time
        self.end_time = end_time
        self.runtime = runtime
        
        
    
    