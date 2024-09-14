class Instance():
    machine_id: int
    cpu: float = 1
    memory: float = 1
    
    def __init__(self, machine_id) -> None:
        self.machine_id = machine_id
        self.cpu = 1
        self.memory = 1
        
    