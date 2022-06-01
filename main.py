from secrets import file_
import pandas as pd
import numpy as np
import os

#postgres wrapper
import psycopg2

#Mapping between dataframe dtypes and postgres
dtype_mappings = {
    'int64': 'int'
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

if __name__ == "__main__":
    preproc(file_)