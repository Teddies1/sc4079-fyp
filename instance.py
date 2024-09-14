from vm_task import Task

class Instance():
    machine_id: int
    core_capacity: float = 1
    memory_capacity: float = 1
    list_of_tasks: list[Task]
    starttime: float
    endtime: float
    max_runtime: float = 0
    
    def __init__(self, machine_id: int) -> None:
        self.machine_id = machine_id
        self.list_of_tasks = []
    
    def get_max_runtime(self) -> float:
        if len(self.list_of_tasks) > 0:
            self.max_runtime = max(self.list_of_tasks, key=lambda x: x.runtime)
        
        return self.max_runtime
    
    
        
        
    