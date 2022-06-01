from secrets import file_, host, dbname, user, passw
import pandas as pd
import numpy as np
import os

#postgres wrapper
import psycopg2

#Mapping between dataframe dtypes and postgres
dtype_mappings = {
    'int64': 'int',
    'object': 'varchar'
}

def pre_proc(file_):
    #read csv into dataframe
    try:
        df = pd.read_csv(file_)
    except:
        print("Check CSV file path")

    #ensure column formatting is consistent
    df.columns = [x.lower().replace(" ", "").replace("-","_")
                    .replace(r"/","_").replace("\\","_").replace("$","")
                    .replace("%","") for x in df.columns]

    print(df.dtypes)
    return df

def create_dbt_format(df, dtype_mappings):
    #format in a sql-like structure, while replacing the dataframe datatypes into postgres datatypes
    f_columns = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(dtype_mappings)))
    print(f_columns)

    return f_columns

def db_connection(host, db, user, passw):

    conn = psycopg2.connect(database=db,
                            user=user,
                            password=passw,
                            host=host,
                            connect_timeout=3)

    print('opened database successfully')

    #returns a cursor to perform database operations
    return conn.cursor()

if __name__ == "__main__":
    df = pre_proc(file_)
    table_f = create_dbt_format(df, dtype_mappings)

    print(host, dbname, user, passw)
    cursor = db_connection(host, dbname, user, passw)