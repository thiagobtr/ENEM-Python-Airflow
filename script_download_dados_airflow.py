# libs utilizadas
import urllib.request
import pandas as pd
from zipfile import ZipFile
from sqlalchemy import create_engine
from configparser import ConfigParser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Funcao para leitura do arquivo "database.ini", com os parametros de conexao
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def conexao_BD ():
	# cria variavel com os parametros
	params = config()

	# Faz a leitura dos parametros de conexao do BD
	url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
    user=params['user'], passwd=params['password'], host=params['host'], port=5432, db=params['database'])

	alchemyEngine = create_engine(url, pool_size = 50)

	# Realiza a conexao
	dbConn = alchemyEngine.connect();
	
	return dbConn

def download_dados():
	# URL microdados Enem 2019
	url= 'https://download.inep.gov.br/microdados/microdados_enem_2019.zip'
	path= '/usr/src/app/docker'
	filepath= '/usr/src/app/docker/MICRODADOS_ENEM_2019.zip'
	
	# faz o download e salva o arquivo
	urllib.request.urlretrieve(url,filepath)

	# Extracao arquivo zip
	with ZipFile(filepath, 'r') as zipObj:
    		# extraindo conteudo para o diretorio 
    		zipObj.extractall(path)
	
	
	return 'Step 1 - OK' 

def leitura_dados(**kargs):

	# recupera as variaveis da funcao anterior
 	conn = kargs['task_instance'].xcom_pull(key=None,task_ids='python_task-2')

	# leitura do arquivo "MICRODADOS_ENEM_2019" (top 50k)
	df_enem = pd.read_csv(path+'DADOS/MICRODADOS_ENEM_2019.csv', nrows= 50000, encoding= 'latin-1',sep=';')

	# leitura do arquivo "ITENS_PROVA_2019"
	df_itens_prova = pd.read_csv(path+'DADOS/ITENS_PROVA_2019.csv', encoding= 'latin-1',sep=';')

	return df_enem ,df_itens_prova ,conn
	
def load_dados_db(**kargs):
	
	# recupera as variaveis da funcao leitura_dados()
	# vars[0]-> df_enem
	# vars[1]-> df_itens_prova
	# vars[3]-> conexao bd
	vars = kargs['task_instance'].xcom_pull(key=None,task_ids='python_task-3')
	
	# Criacao das tabelas no BD

	# tabela MICRODADOS
	vars[0].to_sql('MICRODADOS',con=vars[3],if_exists='replace')
	# tabela ITENS_PROVA
	vars[1].to_sql('ITENS_PROVA',con=vars[3],if_exists='replace')
	
	return 'Step 3 - OK'

# Criacao arquivo DAG
with DAG('Enem_dados_python_dag', schedule_interval='0 0 * * *', 
         start_date=datetime(2021, 01, 10), catchup=False) as dag:
	enem_python_1 = PythonOperator(task_id='python_task-1', python_callable=download_dados )
	enem_python_2 = PythonOperator(task_id='python_task-2', python_callable=conexao_BD)
	enem_python_3 = PythonOperator(task_id='python_task-3', python_callable=leitura_dados)
	enem_python_4 = PythonOperator(task_id='python_task-4', python_callable=load_dados_db, provide_context=True)

	enem_python_1 >> enem_python_2 >> enem_python_3 >> enem_python_4
