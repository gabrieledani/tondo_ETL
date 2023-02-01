import configparser
import pandas as pd
import datetime
import warnings

#bibliotecas de conexao com bancos de dados
import sqlalchemy
import fdb
import cx_Oracle
import pyodbc
import psycopg2 as pg

config = configparser.ConfigParser()
config.read('config.ini')

# In[ ]:Conexao com banco Firebird
def postgresql_connect():
    p_host = config['POSTGRESQL']['host']
    p_database = config['POSTGRESQL']['database']
    p_user = config['POSTGRESQL']['username']
    p_password = config['POSTGRESQL']['password']
   
    connection = pg.connect(host=p_host, database=p_database, user=p_user, password=p_password)

    return connection

# In[ ]:Conexao com banco Firebird
def firebird_connect():
    #warnings.filterwarnings('ignore')
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
    driver = config['SQLSERVER']['Driver']
    server = config['SQLSERVER']['Server']
    database = config['SQLSERVER']['Database']
   
    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes;')
    return connection

# In[ ]:Conexao Oracle
def oracle_connect():
    #warnings.filterwarnings('ignore')
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
    #warnings.filterwarnings('ignore')
    username = config['MYSQL']['username']
    password = config['MYSQL']['password']
    port = config['MYSQL']['port']
    hostname = config['MYSQL']['hostname']
    schema_name = config['MYSQL']['schema_name']

    #print("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)
    engine = sqlalchemy.create_engine("mysql+pymysql://"+username+":"+password+"@"+hostname+":"+port+"/"+schema_name)
    return engine

# In[ ]:Carga de Dados para o DataWareHouse
def load_Cargas(queries_db, datawarehouse):
    print('Loading Cargas....')
    print(datetime.datetime.today())

    print(queries_db, datawarehouse)

    #busca todas as tabelas que dos modulos do BI
    sql_str = "SELECT id, tabela, string_ins, string_del, db_origem, tipo FROM queries where tipo = 'F' order by sequencia asc;"
    #print(sql_str)

    if queries_db == 'SQLSERVER':
        conn_dw = sqlserver_connect()
        cursor = conn_dw.cursor()
        queries = cursor.execute(sql_str).fetchall()
    elif queries_db == 'ORACLE':
        conn_dw = oracle_connect()
        queries = pd.read_sql(sql_str,conn_dw)
    elif queries_db == 'MYSQL':
        conn_dw = mysql_connect()
        queries = pd.read_sql(sql_str,conn_dw)
    elif queries_db == 'FIREBIRD':
        conn_dw = firebird_connect()
        queries = pd.read_sql(sql_str,conn_dw)
    elif queries_db == 'POSTGRESQL':
        conn_dw = postgresql_connect()
        queries = pd.read_sql(sql_str,conn_dw)
    
    #print(queries)

    print("Exclusões")
    for data in queries:
        #print(data)
        if queries_db == 'SQLSERVER':
            cur_excl = conn_dw.cursor()
            print(str(data[3]))
            cur_excl.execute(str(data[3]))
            conn_dw.commit()
        else:
            conn_dw.execute(sql = str(data[3]))
    
    conn_dw.close()
    
    for data in queries:
        print(data)
        querie_id = str(data[0])
        tabela = str(data[1])
        string_ins = str(data[2])
        #string_del = str(data[3])
        db_origem = str(data[4])

        tipo = str(data[5])

        #Busca os dados de extração da origem conforme tipo
        if db_origem == 'csv':
            print("[Before BULK INSERT...]")
            bulkInsertCommand = 'BULK INSERT '+tabela+" FROM '"+string_ins+"' WITH (FIRSTROW = 2, FIELDTERMINATOR =';',ROWTERMINATOR ='\n');"
        else:
            if db_origem == 'ORACLE':
                conn_ori = oracle_connect()
            elif db_origem == 'MYSQL':
                conn_ori = mysql_connect()
            elif db_origem == 'FIREBIRD':
                conn_ori = firebird_connect()
            elif db_origem == 'POSTGRESQL':
                conn_ori = postgresql_connect()
            print('read_sql')
            data_extract = pd.read_sql(string_ins, conn_ori)
            
        if db_origem == 'csv' and datawarehouse == 'SQLSERVER':
            conn_dw = sqlserver_connect()
            cursor = conn_dw.cursor()

            print("Running BULK INSERT command...")    
            cursor.execute(bulkInsertCommand)
            conn_dw.commit()
            print("BULK INSERT command executed.")    
        else:
            #executa a inclusao no DW
            data_extract.to_sql(tabela, con=conn_dw, if_exists='append', index=False, chunksize=10000)

    #executa o update na tabela para infrormar a data de atualização da mesma
    data_atual = datetime.datetime.today() 
    sqlUp = "update queries set dt_carga = '"+ str(data_atual) + "' where id = " + querie_id
    #print(sqlUp)
    conn_dw.execute(sqlUp)
    conn_dw.execute('commit')

#while True:
print('Executando sincronismo ERP x BI...')
print(datetime.datetime.now())

#Busca Queries
banco_queries = config['DEFINITIONS']['bd_queries']
banco_dw = config['DEFINITIONS']['bd_dw']

load_Cargas(banco_queries,banco_dw)

print("Encerrado o sincronismo...")
print(datetime.datetime.now())
    
