import datetime
#bibliotecas de conexao com bancos de dados
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pandas as pd

# In[ ]:Conexao com DataWareHouse 
def sqlserver_connect_dw(server_db):
    if server_db == 'MSSQL':
        server = 'tcp:mssql.orquidea.com.br'
    elif server_db == 'MSSQL2':
        server = 'tcp:mssql2.orquidea.com.br'

    database = 'dw_tondo_bi' 
    username = 'hunterBI' 
    password = 'lH43r#BA8dKb'

    connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+password
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    
    return engine

def progress_connect_origem():
    cnxn = pyodbc.connect('DRIVER={Progress OpenEdge 11.7 Driver};HostName=progress.orquidea.com.br;DATABASENAME=ems2cad;PORTNUMBER=20101;LogonID=sysprogress;PASSWORD=sysprogress')
    cursor = cnxn.cursor()

    return cursor
    #return cnxn

conprog = progress_connect_origem()
conprog.execute('SELECT "cod-estabel",nome FROM pub."estabelec"')
rows = conprog.fetchall()
print(rows)

df = pd.DataFrame((tuple(t) for t in rows),index=None) 

print(df)
'''
sql_str = "SELECT id, tabela, string_ins, string_del, db_origem, tipo, separador, tp_origem FROM queries where id in (4,20) order by sequencia asc;"
conn_dw = sqlserver_connect_dw('MSSQL')
data = pd.read_sql(sql_str,conn_dw)
'''