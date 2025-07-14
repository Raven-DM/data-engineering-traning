# %%
import os
import glob
import json
import re
import pandas as pd

# %%
def get_column_names(schemas, table, key = 'column_position'):
    column_details = schemas[table]
    columns = sorted(column_details, key = lambda col: int(col[key]))
    return [col['column_name'] for col in columns]

# %%
def read_csv(file,schemas):
    file_path = re.split('[/\\\\]', file)
    table_name = file_path[-2]
    return pd.read_csv(file, header=None, names=get_column_names(schemas, table_name))    

# %%
def to_json(df, target_base_dir, table_name, file_name):
    json_file_path = f'{target_base_dir}/{table_name}/{file_name} '
    print(f'Processing {json_file_path}')
    os.makedirs(f'{target_base_dir}/{table_name}', exist_ok=True)
    df.to_json(json_file_path, orient='records', lines=True)    

# %%
def file_converter(src_base_dir, target_base_dir, table_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{table_name}/part-*')
    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\\]', file)[-1]
        to_json(df, target_base_dir, table_name, file_name)

# %%
def process_files():
    src_base_dir = 'data/retail_db'
    target_base_dir = 'data/retail_db_json'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    table_names = schemas.keys()
    for table_name in table_names:
        print(f'Processing table: {table_name}')
        file_converter(src_base_dir, target_base_dir, table_name)

# %%
process_files()


