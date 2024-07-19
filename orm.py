import sqlalchemy
import pandas as pd

query = 'select vm.vmId, vm.tenantId, vm.vmTypeId, vm.priority, vm.starttime, vm.endtime, vmType.core, vmType.memory from vm, vmType where vm.vmTypeId = vmType.vmTypeId and endtime <= 14 and machineId = 16;'
path = 'sqlite:///../db/azure_packing_trace.db'
    
def get_collated_data(query):    
    engine=sqlalchemy.create_engine(path)
    connection = engine.connect()

    results = pd.read_sql_query(query, connection)

    connection.close()
    return results

res = get_collated_data(query)
print(res)