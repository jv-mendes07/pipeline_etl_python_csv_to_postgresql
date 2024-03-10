# Importação da biblioteca sys para definir variáveis de ambiente na aplicação Python:

import sys

# Importação da biblioteca glob para capturar dinamicamente o nome dos arquivos dos datasets:

import glob

# Importação da biblioteca os para criar diretórios, pastas e subpastas:

import os

# Importação da biblioteca JSON para lidar com arquivos JSON:

import json

# Importação da biblioteca re para trabalhar com expressão regulares:

import re

# Importação da biblioteca Pandas para manipulação e tratamento de dados:

import pandas as pd

# Importação da biblioteca psycopg2 para realizar truncamento dos dados antes de executar a Pipeline FULL
# de ingestão de dados dos arquivos CSV da fonte para tais tabelas de destino no PostGreSQL:

import psycopg2

# Função Python para capturar o nome das colunas de uma tabela no arquivo JSON de metadados:

def get_column_names(schemas, ds_name, sorting_key = 'column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key = lambda col : col[sorting_key])
    return [col['column_name'] for col in columns]

# Função Python para importar o arquivo CSV da fonte do diretório de arquivos CSV armazenados:

def read_csv(file, schemas):
    file_path_list = re.split('[\\\\/]', file)
    ds_name = file_path_list[-2]
    file_name = file_path_list[-1]
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names = columns, chunksize = 10000)
    return df_reader

# Função Python que irá realizar transformações e tratamentos mínimos em determinados arquivos CSV importados:

def transform_csv(df, ds_name):
    if ds_name == 'customers':
        df['customer_full_name'] = df['customer_fname'] + ' ' + df['customer_lname']
        return print(f'Transformation of {ds_name}')
    elif ds_name == 'orders':
        df['order_status'] = df['order_status'].str.capitalize()
        df['order_status'] = df['order_status'].str.replace('_', ' ')
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['order_date'] = df['order_date'].dt.date
        return print(f'Transformation of {ds_name}')

# Função que irá truncar todos os dados das tabelas de destino no PostGreSQL antes da execução da Pipeline FULL
# de ingestão de dados dos arquivos CSV da fonte para tais tabelas de destino:
            
def truncate_tables_sql(ds_name):

    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')

    conn = psycopg2.connect(
    host = db_host,
    database = db_name,
    port = db_port,
    user = db_user,
    password = db_pass
    )

    cur = conn.cursor()

    cur.execute(f'TRUNCATE TABLE {ds_name};')
    conn.commit()

# Função que irá carregar os dados do Pandas DataFrame para a tabela no Banco de Dados no PostGreSQL:

def to_sql(df, db_conn_uri, ds_name):
    df.to_sql(
        ds_name,
        db_conn_uri,
        if_exists = 'append',
        index = False
        )
    
# Função que irá iterar sobre os arquivos CSV para carrega-los em pedaços na tabela de destino no PostGreSQL:
    
def db_loader(src_base_dir, db_conn_uri, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    
    for file in files:
        df_reader = read_csv(file, schemas)
        for idx, df in enumerate(df_reader):
            transform_csv(df, ds_name)
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_uri, ds_name)


# Função que irá capturar às variáveis de ambiente para realizar a conexão com o banco de dados PostGreSQL
# para carregar os dados dos arquivos CSV para o banco de dados PostGreSQL:

def process_files(ds_names = None):

    src_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')

    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

    schemas = json.load(open(f'{src_base_dir}/schemas.json'))

    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Truncate Data of {ds_name}')
            truncate_tables_sql(ds_name)
            print(f'Processing {ds_name}')
            db_loader(src_base_dir, db_conn_uri, ds_name)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'Data Processing of {ds_name} is complete')

# Execução de extração de arquivos CSV da fonte para o carregamento no destino da tabela no banco de dados PostGreSQL:

if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()