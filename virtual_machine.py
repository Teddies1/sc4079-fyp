class VirtualMachine:
    vm_type_id: int
    requested_core: float
    requested_memory: float
    
    def __init__(self, vm_type_id: int, requested_core: float, requested_memory: float) -> None:
        self.vm_type_id = vm_type_id
        self.requested_core = requested_core
        self.requested_memory = requested_memory
        
    
        