import sqlalchemy
import pandas as pd

query = 'select vm.vmId as taskId, vm.tenantId, vm.vmTypeId, vm.starttime, vm.endtime, (vm.endtime - vm.starttime) as runtime, vmType.core as requested_core, vmType.memory as requested_memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16;'
path = 'sqlite:///../db/azure_packing_trace.db'
    
def get_collated_data(query, db_path):    
    engine=sqlalchemy.create_engine(db_path)
    connection = engine.connect()

    results = pd.read_sql_query(query, connection)
    results.to_csv("../outputs/tasklist.csv", index=False)
    connection.close()
    
    return results

res = get_collated_data(query, path)
