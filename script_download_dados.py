# libs utilizadas
import urllib.request
import pandas as pd
from zipfile import ZipFile
from sqlalchemy import create_engine
from configparser import ConfigParser

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

def main ():
	# cria variavel com os parametros
	params = config()

	# Faz a leitura dos parametros de conexao do BD
	url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
    user=params['user'], passwd=params['password'], host=params['host'], port=5432, db=params['database'])

	alchemyEngine = create_engine(url, pool_size = 50)

	# Realiza a conexao
	dbConn = alchemyEngine.connect();

	# URL microdados Enem 2019
	url= 'https://download.inep.gov.br/microdados/microdados_enem_2019.zip'
	path= '/usr/src/app/docker/'
	filepath= '/usr/src/app/docker/MICRODADOS_ENEM_2019.zip'
	# faz o download e salva o arquivo
	urllib.request.urlretrieve(url,filepath)

	# Extracao arquivo zip
	with ZipFile(filepath, 'r') as zipObj:
    		# extraindo conteudo para o diretorio 
    		zipObj.extractall(path)

	# leitura do arquivo "MICRODADOS_ENEM_2019" (top 50k)
	enem = pd.read_csv(path+'DADOS/MICRODADOS_ENEM_2019.csv', nrows= 50000, encoding= 'latin-1',sep=';')

	# leitura do arquivo "ITENS_PROVA_2019"
	itens_prova = pd.read_csv(path+'DADOS/ITENS_PROVA_2019.csv', encoding= 'latin-1',sep=';')

	# Criacao das tabelas no BD

	# tabela MICRODADOS
	enem.to_sql('MICRODADOS',con=dbConn,if_exists='replace')
	# tabela ITENS_PROVA
	itens_prova.to_sql('ITENS_PROVA',con=dbConn,if_exists='replace')
	
	return 'OK'


if __name__ == "__main__":
	main()
