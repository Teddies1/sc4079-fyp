class Machine():
    machine_id: int
    cpu: float
    memory: float
    
    def __init__(self, machine_id: int, cpu: float, memory: float) -> None:
        self.machine_id = machine_id
        self.cpu = cpu
        self.memory = memory
        
    