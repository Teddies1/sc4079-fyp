from vm_task import Task

class Instance():
    unique_id: int
    machine_id: int
    core_capacity: float = 1
    memory_capacity: float = 1
    list_of_tasks: list[Task]
    starttime: float
    endtime: float
    max_runtime: float = 0
    
    def __init__(self, unique_id: int, machine_id: int) -> None:
        self.unique_id = unique_id
        self.machine_id = machine_id
        self.list_of_tasks = []
    
    def get_max_runtime(self) -> float:
        if len(self.list_of_tasks) > 0:
            max_runtime_task = max(self.list_of_tasks, key=lambda x: x.runtime)
            self.max_runtime = max_runtime_task.runtime
        else:
            self.max_runtime = float(0)
            
        return self.max_runtime
    
    
        
        
    