import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine

engine=sqlalchemy.create_engine('sqlite:///../db/azure_packing_trace.db')
connection = engine.connect()

df = pd.read_sql_query('select * from vmType', connection)
print(df)

