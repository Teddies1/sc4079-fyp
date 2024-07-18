-- SQLite
select 
vm.vmId, 
vm.tenantId, 
vm.priority, 
vm.starttime, 
vm.endtime, 
(vm.endtime - vm.starttime) as runtime,
vmType.core,
vmType.memory
from vm, vmType 
where endtime <= 14 
and machineId = 16;