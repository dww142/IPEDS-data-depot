import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
import sys

current_directory = os.path.dirname(__file__)
# import settings from another directory; add IPEDS directory to path to import
sys.path.insert(0, os.path.join(current_directory, '..', 'IPEDS'))
import settings

for file in os.listdir(os.path.join(current_directory, 'Data')):
    file_path = os.path.join(current_directory, 'Data', file)
    schema, table = file.split('.')[0:2]
    df = pd.read_csv(file_path, dtype=object)
    
    trunc_statement = f'DELETE FROM {schema}.{table}'
    print(f'{schema}.{table} data deleted', end='...')
    settings.ENGINE.execute(trunc_statement)
    df.to_sql(table, settings.ENGINE, schema=schema, if_exists='append', index=False)
    print(file, f'loaded to {schema}.{table}')
