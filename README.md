# ENEM - Coleta de dados 
Estrutura do diretório dos arquivos localizados:

![tree.jpg](https://github.com/thiagobtr/data-challenge/blob/main/images/tree.jpg)


## Descrição dos arquivos

**database.ini** -> arquivo com os dados para conexão do banco de dados<br>
**Dockerfile** -> script com a criação do serviço "app-python" para download dos dados<br>
**docker-compose.yml** -> arquivo com os serviços "db" (PostgreSQL) e app-python (aplicação python)<br>
**requirements.txt** -> arquivo com as bibliotecas necessárias para a execução do script python<br>
**script_download_dados.py** -> script python para download e inclusão dos dados no banco de dados<br>
**script_download_dados_airflow.py** -> versão do script python usando airflow. Indicado para agendamento e monitoramento do script.<br>
**scripts_sql.sql** -> script sql com as consultas dos itens 2 e 3.<br>

## Execução
Com o docker instalado e os arquivos baixados, execute: <br>
```
docker-compose up -d
```
