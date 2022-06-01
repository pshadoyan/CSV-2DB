from secrets import file_
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

def preproc(file_):
    #read csv into dataframe
    df = pd.read_csv(file_)

    #ensure column formatting is consistent
    df.columns = [x.lower().replace(" ", "").replace("-","_")
                    .replace(r"/","_").replace("\\","_").replace("$","")
                    .replace("%","") for x in df.columns]

    print(df.dtypes)
    return df

def create_dbt_format(df, dtype_mappings):
    f_columns = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(dtype_mappings)))
    print(f_columns)

if __name__ == "__main__":
    df = preproc(file_)
    create_dbt_format(df, dtype_mappings)