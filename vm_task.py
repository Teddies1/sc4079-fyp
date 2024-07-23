from virtual_machine import VirtualMachine

class Task:
    vm: VirtualMachine
    task_id: int
    start_time: float
    end_time: float
    runtime: float
    running: bool = False
    
    def __init__(self, vm: VirtualMachine, task_id: int, runtime: float, start_time: float, end_time: float) -> None:
        self.vm = vm
        self.task_id = task_id
        self.vm_type_id = vm.vm
        self.start_time = start_time
        self.end_time = end_time
        self.runtime = runtime
        
        
    
    