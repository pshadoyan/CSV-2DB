from secrets import file_, host, dbname, user, passw
import pandas as pd
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

#utility function to format the name and types of the columns
def create_dbt_format(df, dtype_mappings):
    #format in a sql-like structure, while replacing the dataframe datatypes into postgres datatypes
    f_columns = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(dtype_mappings)))
    print(f_columns)

    return f_columns

def db_upload(host, db, user, passw, table_f, df, table_name):

    #connect to the Amazon RDS via postgresql wrapper
    conn = psycopg2.connect(database=db,
                            user=user,
                            password=passw,
                            host=host,
                            connect_timeout=3)

    print('opened database successfully')

    #returns a cursor to perform database operations, a cursor allows 
    #PostgreSQL commands in a successful database connected session
    cursor = conn.cursor()

    #If there is an existing database of the same name, remove it
    cursor.execute("drop table if exists "+table_name+";")

    cursor.execute("create table customer_info ("+table_f+");")

    #Convert the dataframe into a local csv file
    #index=False, ensure there isn't a duplicate csv index
    df.to_csv('customer_info.csv', header=df.columns, index=False, encoding='utf-8')

    try:
        c_info = open('customer_info.csv')
    except:
        print('file missing')
    
    SQL_c = """
    copy customer_info from STDIN with
        CSV
        HEADER
        DELIMITER AS ','
    """

    #this allows a user-composed copy statement to be submitted
    #COPY moves data between postgresql tables and standard file-system files
    cursor.copy_expert(sql=SQL_c, file=c_info)

    #commits any pending transaction to the database
    #if this isn't called, the effect of any data manipulation will be lost
    conn.commit()

    cursor.close()
    conn.close()
    
if __name__ == "__main__":
    df = pre_proc(file_)
    table_f = create_dbt_format(df, dtype_mappings)

    db_upload(host, dbname, user, passw, table_f, df, "customer_info")