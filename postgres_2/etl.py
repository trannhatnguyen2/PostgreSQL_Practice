# import needed libraries
from sqlalchemy import create_engine
import pyodbc
import pandas as pd 
import os
import psycopg2

# get password from environment var
# pwd = os.environ['PGPASS']
# uid = os.environ['PGUID']
pwd = 'nhatnguyen18'
uid = 'trannhatnguyen2'


# sql db details
driver = "{SQL Server}"
server = "LAPTOP-80UMU1UQ\\NHATNGUYEN"
database = "BI_DW_BoKho_K20406C"
port = '1433'

# extract data from sql server
def extract():
    try: 
        # src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\\SQLEXPRESS' + ';DATABASE=' + database + ';TRUSTED_CONNECTION=True;PORT=1433' + ';USER ID=' + uid + ';PASSWORD=' + pwd)
        
        # src_conn = pyodbc.connect('SERVER=LAPTOP-80UMU1UQ\\NHATNGUYEN;DATABASE=BI_DW_BoKho_K20406C;TRUSTED_CONNECTION=True;PORT=1433;DRIVER={SQL Server};USER ID=trannhatnguyen2;PASSWORD=nhatnguyen18')
        src_conn = pyodbc.connect('SERVER=' + server + ';DATABASE=' + database + ';TRUSTED_CONNECTION=True;PORT=' + port + ';DRIVER=' + driver + ';USER ID=' + uid + ';PASSWORD=' + pwd)
        src_cursor = src_conn.cursor()

        # execute query
        src_cursor.execute(
            """
            SELECT t.name AS table_name
            FROM sys.tables AS t 
            WHERE t.name IN ('DimChannel', 'DimCustomer', 'DimDate', 'DimEmployee', 'DimGeography', 'DimProduct', 'DimShipMethod', 'DimStore', 'FactSales')
            """)
        src_tables = src_cursor.fetchall()

        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'SELECT * FROM DW.{tbl[0]}', src_conn)
            print(df)
            load(df, tbl[0])

    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

# load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://trannhatnguyen2:nhatnguyen18@localhost:5432/BI_DW_BoKho_K20406C')

        print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')

        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)

        # add elapsed time to final print out
        print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))


try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))






