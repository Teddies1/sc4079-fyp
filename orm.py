import sqlalchemy
import pandas as pd

path = 'sqlite:///../db/azure_packing_trace.db'

instance_query = 'select vm.vmTypeId, count(vm.vmId) as taskcount, max((vm.endtime - vm.starttime)) as maxruntime, vmType.core, vmType.memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16 group by vm.vmTypeId;'
task_query = 'select vm.vmId as taskId, vm.tenantId, vm.vmTypeId, vm.starttime, vm.endtime, (vm.endtime - vm.starttime) as runtime, vmType.core as requested_core, vmType.memory as requested_memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16;'

instance_output = "../outputs/vmlist.csv"
task_output = "../outputs/tasklist.csv"

def get_collated_data(query, db_path, output_path):    
    engine=sqlalchemy.create_engine(db_path)
    connection = engine.connect()

    results = pd.read_sql_query(query, connection)
    results.to_csv(output_path, index=False)
    connection.close()
    
    return results

res = get_collated_data(task_query, path)
vmlist = get_collated_data(instance_query, path, instance_output)
