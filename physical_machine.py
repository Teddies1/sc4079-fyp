class Machine(object):
    machine_id: int
    core_capacity: float
    memory_capacity: float
    queue: list
    
    def __init__(self, queue: list, machine_id: int=16, core_capacity: float=1, memory_capacity: float=1) -> None:
        self.machine_id = machine_id
        self.core_capacity = core_capacity
        self.memory_capacity = memory_capacity
        self.queue = queue
        
    def add_instance():
        pass
    
    def remove_instance():
        pass
    
    