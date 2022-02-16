import os
import pandas as pd
from cryptography.fernet import Fernet
import configparser
from sqlalchemy import create_engine
import dbconnect
import initvar

def loadcsv():
    df = pd.read_csv(os.path.abspath('data\\ref.csv'))
    #print(df.head())

    try:
        df.to_sql(con=dbconnect.engine, name='refdata', if_exists='replace')
        print(' *** COMPLETED LOADING REFERENCE DATA *** ')
    except TypeError as e:
        print(e)
        return None
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(e)
    finally:
        #print("MySQL Connection is closed")
        return


def main():
    dbconnect.configconnection()
    loadcsv()
