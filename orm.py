import sqlalchemy
import pandas as pd

path = 'sqlite:///../db/azure_packing_trace.db'

instance_types_query = 'select vm.vmTypeId, count(vm.vmId) as taskcount, (max((vm.endtime - vm.starttime)) * 86400) as maxruntime, vmType.core, vmType.memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16 group by vm.vmTypeId;'
task_query = 'select vm.vmId as taskId, vm.tenantId, vm.vmTypeId, (vm.starttime * 86400) as starttime , (vm.endtime * 86400) as endtime, ((vm.endtime - vm.starttime)*86400) as runtime, vmType.core as requested_core, vmType.memory as requested_memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16 order by starttime;'
instance_query = 'select vm.vmTypeId, count(vm.vmId) as taskcount, vm.starttime * 86400 as starttime, vm.endtime * 86400 as endtime, (86400  * max((vm.endtime - vm.starttime))) as maxruntime, vmType.core, vmType.memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16 group by starttime, vm.vmTypeId;'

instance_types_output = "../outputs/vmlist2.csv"
task_output = "../outputs/tasklist2.csv"
instance_output = "../outputs/assignedinstancelist2.csv"

def get_collated_data(query, db_path, output_path):    
    engine=sqlalchemy.create_engine(db_path)
    connection = engine.connect()

    results = pd.read_sql_query(query, connection)
    results.to_csv(output_path, index=False)
    connection.close()
    
    return results

res = get_collated_data(task_query, path, task_output)
vm_type_list = get_collated_data(instance_types_query, path, instance_types_output)
vm_list = get_collated_data(instance_query, path, instance_output)
