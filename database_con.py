from pathlib import Path
import os
import configparser
import warnings
import datetime
import pandas as pd

#bibliotecas de conexao com bancos de dados
import pyodbc
import sqlalchemy
import pymysql
import fdb

import cx_Oracle

config = configparser.ConfigParser()
config.read('config.ini')

# In[ ]:Conexao com banco Firebird
def firebird_connect():
    warnings.filterwarnings('ignore')
    fdb.load_api(config['FIREBIRD']['dll_path'])
    connection = fdb.connect(
        database=config['FIREBIRD']['file'],
    user=config['FIREBIRD']['user'],
    password=config['FIREBIRD']['password'],
    host=config['FIREBIRD']['host'],
    port=int(config['FIREBIRD']['port']),
    role=config['FIREBIRD']['role'],
    charset=config['FIREBIRD']['charset']
    )

    '''opção 2
    import firebirdsql

    conn = firebirdsql.connect(
        host='177.38.158.47',
        database=r'\syspro\bd\sysdb.fbd',
        port=3050,
        user='USR_BI_RH_SCH',
        password='sieccon456#',
        role='RLBIRHSCH',
        charset='UTF8'
    )
    cur = conn.cursor()

    cur.execute("SELECT * FROM VW_BI_RH_SCH")

    for c in cur.fetchall():
        print(c)

    conn.close()
    '''
    return connection


# In[ ]:Conexao com banco Sql Server
def sqlserver_connect():
    warnings.filterwarnings('ignore')
    #Conexao com banco usando autenticacao do Windows
    #parametros da funcao connect = Driver, Nome do Servidor, Banco de Dados, Autenticacao
    connection = pyodbc.connect("Driver="+config['SQLSERVER']['Driver']+";"
                                "Server="+config['SQLSERVER']['Server']+";"
                                "Database="+config['SQLSERVER']['Database']+";"
                                "Trusted_Connection="+config['SQLSERVER']['Trusted_Connection']+"")

    #Conexao com banco usando autenticacao com usuário e senha
    #parametros da funcao connect = Driver, Nome do Servidor, Banco de Dados, Autenticacao
    '''
    connection_u = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                                "Server=localhost;"
                                "Database=dw_bi;"
                                "UID=username;"
                                "PWS=password;")
    '''
    return connection


# In[ ]:Conexao Oracle
def oracle_connect():
    warnings.filterwarnings('ignore')
    user = config['ORACLE']['user']
    password = config['ORACLE']['password']
    ip = config['ORACLE']['ip']
    sid = config['ORACLE']['sid']

    connection = cx_Oracle.connect(
        user,
        password,
        f'{ip}/{sid}',
        encoding="UTF-8")
    return connection

# In[ ]:Conexao MySQL
def mysql_connect():
    warnings.filterwarnings('ignore')
    username = config['MYSQL']['username']
    password = config['MYSQL']['password']
    port = config['MYSQL']['port']
    hostname = config['MYSQL']['hostname']
    schema_name = config['MYSQL']['schema_name']

    #print("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)
    engine = sqlalchemy.create_engine("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)
    return engine


'''
#Importação para DataFrame
conn_ss = sqlserver_connect()
sql_txt = "select * from dbo.queries"
df1 = pd.read_sql(sql_txt,conn_ss)
print(df1)
#Fechando a conexao
conn_ss.close()
'''

#c_ora = ora_database_connect()
#cursor = connect_ora.cursor()

#Create sqlalchemy engine
#mysql_e = mysql_database_connect()

#load_dimensions(c_ora,mysql_e)

#load_fato(c_ora,mysql_e)

#close connection
#c_ora.close()
#mysql_e.dispose()

'''cursor.execute("""
        SELECT COD_EMP, RAZAO_SOCIAL FROM TEMPRESAS 
        WHERE id > :eid""",
        eid = 1)
for fname, lname in cursor:
    print("Values:", fname, lname)
'''
#connection = cx_Oracle.connect(user="focco3i", password="cachimbo", dsn="192.168.1.253/f3ipro")
'''
con = firebird_connect()
cur = con.cursor()

# Execute the SELECT statement:
cur.execute("SELECT EMP_COD  FROM GER_EMPRESAS ge ")

# Retrieve all rows as a sequence and print that sequence:
for c in cur.fetchall():
    print(c)

con.close()
'''