import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pandas as pd
import datetime
import os

# In[ ]:Conexao com DataWareHouse - SQL Server
def sqlserver_connect_dw():
    server = 'tcp:mssql2.orquidea.com.br'
    database = 'dw_tondo_bi' 
    username = 'hunterBI' 
    password = 'lH43r#BA8dKb'

    connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+password
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    
    return engine

# In[ ]:Conexao com Banco de Dados de Origem - SQL Server
def sqlserver_connect_origem(server_db):
    if server_db == 'MSSQL':
        server = 'tcp:mssql.orquidea.com.br'
        username = 'hunterBI' 
        password = 'lH43r#BA8dKb'
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";UID="+username+";PWD="+password

    elif server_db == 'MSSQL2':
        server = 'tcp:mssql2.orquidea.com.br'
        database = 'dw_tondo_bi' 
        username = 'hunterBI' 
        password = 'lH43r#BA8dKb'
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+password
    
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    
    return engine

# In[ ]:Conexao com Banco de Dados de Origem - Progress
def progress_connect_origem(database,port):
    
    server = 'progress12.orquidea.com.br'
    #database = 'ems2cad'
    #port = '20101'
    username = 'sysprogress' 
    password = 'sysprogress'

    '''
    connection_string = "DRIVER={Progress OpenEdge 11.4 Driver};SERVER="+server+";UID="+username+";PWD="+password

    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    
    #return engine
    '''
    connection_url = "HostName="+server+";DATABASENAME="+database+";PORTNUMBER="+port+";LogonID="+username+";PASSWORD="+password
    print(connection_url)
    cnxn = pyodbc.connect("DRIVER={Progress OpenEdge 11.7 Driver};HostName="+server+";DATABASENAME="+database+";PORTNUMBER="+port+";LogonID="+username+";PASSWORD="+password)
    cursor = cnxn.cursor()

    return cursor
    #return cnxn


# In[ ]:Exclusão, Extração e Carga dos dados
def extract_load(df_queries,connection_dw):
    for query in df_queries.itertuples(index=False):
        print(query.tabela)
        #print(query.string_ins)
        #print(query.string_del)
        #print(query.db_origem)
        #print(query.tipo)
        #print(query.separador)
        #print(query.tp_origem)
        
        #Executa a exclusão
        connection_dw.execute(query.string_del)
        
        if query.db_origem == 'PROGRESSCAD':
            connection = progress_connect_origem('ems2cad','10050')
        elif query.db_origem == 'PROGRESSMOV':
            connection = progress_connect_origem('ems2mov','10150')
        elif query.db_origem == 'PROGRESSESP':
            connection = progress_connect_origem('ems2esp','10250')
        else:
            connection = sqlserver_connect_origem(query.db_origem)

        tb_columns = pd.read_sql('SELECT * FROM '+str(query.tabela),connection_dw).columns
        #print(tb_columns)

        #Executa extração
        if query.tp_origem == 'csv':
            absolute_path = os.path.normpath(query.string_ins)
            df_data = pd.read_csv(absolute_path,sep=query.separador.strip(), encoding = 'ISO-8859-1', names=tb_columns,skiprows=1, index_col=False)
            #print('extract')
            #print(df_data.head())

        elif query.tp_origem == 'table':
            if query.db_origem[:8] == 'PROGRESS':
                connection.execute(str(query.string_ins))
                rows = connection.fetchall()
                df_data = pd.DataFrame((tuple(t) for t in rows),index=None,columns=tb_columns) 
            else:
                df_data = pd.read_sql(str(query.string_ins),connection, columns=tb_columns, index_col=False)

            #print('table')
            #print(df_data.head())

        #Executa a inclusao
        df_data.to_sql(query.tabela, con=connection_dw, if_exists='append', index=False, chunksize=10000)

        #executa o update na tabela para infrormar a data de atualização da mesma
        data_atual = datetime.datetime.today() 
        sqlUp = "update queries set dt_carga = '"+ str(data_atual) + "' where id = " + str(query.id)
        connection_dw.execute(sqlUp)

# In[ ]:Main

#Busca Queries
banco_queries = 'MSSQL2'
#banco_dw = 'MSSQL2'

sql_str = "SELECT id, tabela, string_ins, string_del, db_origem, tipo, separador, tp_origem FROM queries where tipo = 'D' and id = 22 order by sequencia asc;"
#sql_str = "SELECT id, tabela, string_ins, string_del, db_origem, tipo, separador, tp_origem FROM queries where id in (20) order by sequencia asc;"
conn_dw = sqlserver_connect_dw()
data = pd.read_sql(sql_str,conn_dw)

extract_load(data,conn_dw)

#print(data)

