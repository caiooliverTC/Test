from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MysqlOperator
from datetime import datetime
import mysql.connector


def conexaogetec():

    conexao2 = {
        'user': 'miguel',
        'password': '?YDJ,SKc}23>myKkjWA',
        'host': '186.216.160.30',
        'database': 'getec'
    }
    # Conectar ao primeiro banco de dados
    conn1 = mysql.connector.connect(**conexao2)
    select1 = conn1.cursor()

    # Executar consulta no primeiro banco de dados
    select1.execute('SELECT ticket_id FROM incidente_glpi WHERE status = 1')
    result1 = select1.fetchall()

    # Fechar a conexão com o primeiro banco de dados
    conn1.close()

def conexaoglpi():
    # Configurações de conexão para o primeiro banco de dados
    conexao1 = {
        'user': 'glpi',
        'password': '909!Mko0i9nj',
        'host': '10.2.199.36',
        'database': 'glpi10'
    }

    # Conectar ao segundo banco de dados
    conn2 = mysql.connector.connect(**conexao1)
    select2 = conn2.cursor()

    # Executar consulta no segundo banco de dados
    print('Resultados do Banco de Dados 2:')
    for row in conexaogetec.result1:
        param = row[0]
        select2.execute('SELECT id, status ,closedate FROM glpi_tickets WHERE status = 6 and id = %s', (param,))
        result2 = select2.fetchall()
        for row in result2:
            print(row)

    # Fechar a conexão com o segundo banco de dados
    conn2.close()


#Objeto DAG
dag = DAG (
    "dag_select",
    start_date = datetime(2023, 5, 21),
    schedule_interval = "10 * * * *",
    catchup = False
) 

select_getec = PythonOperator(
    task_id='select_getec',
    python_callable=conexaogetec
)

select_glpi=PythonOperator(
    task_id='select_glpi',
    python_callable=conexaoglpi
)


select_getec >> select_glpi