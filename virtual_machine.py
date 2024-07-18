class VirtualMachine:
    vm_id: int
    requested_core: float
    requested_memory: float
    
    def __init__(self, vm_id: int, requested_core: float, requested_memory: float) -> None:
        self.vm_id = vm_id
        self.requested_core = requested_core
        self.requested_memory = requested_memory
        
    
        