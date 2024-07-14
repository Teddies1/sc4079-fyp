import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine

engine=sqlalchemy.create_engine('sqlite:///../db/azure_packing_trace.db')
connection = engine.connect()

df1 = pd.read_sql_query('select * from vmType', connection)
print(df1)

df2 = pd.read_sql_query('select * from vm', connection)
print(df2)
