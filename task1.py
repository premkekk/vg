# part 1
import os
import sys
import pickle

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from operator import itemgetter

import yfinance as yf
from datetime import datetime
from sqlalchemy import create_engine

from collections import OrderedDict
import numpy as np
from matplotlib import pyplot as plt

lstSyms = []
connection_string = "mysql+pymysql://%s:%s@%s/%s" % ("vguser", "vgpwd", "localhost", "vgdb")
engine = create_engine(connection_string)

def populateSyms(pListsyms):
    for sym in pListsyms:
        if sym not in lstSyms:
            lstSyms.append(sym)

def processPickleFile():
    pickle_inputfile = os.path.abspath("D:\\projects\\vg\\c.pkl")
    pickle_file = open(pickle_inputfile, "rb")
    up = pickle.load(pickle_file)
    for row in up.itertuples(name='SymData'):
        symDataList = row[1]
        if symDataList:
            l_listsymbols = list(map(itemgetter(0), symDataList))
            populateSyms(l_listsymbols)
            # symAssetClass=list(map(itemgetter(3), symDataList))
            # print("Row:{} ; Date:{}; Symbols:{}; AssetClass :{}".format( r, row[0], syms, symAssetClass ) )
            # datetime.strptime(row[0],'%Y-%m-%d %H:%M:%S')


def dmlMySQLDB(sql):
    try:
        mysqlconnection = mysql.connector.connect(
            host="localhost",
            user="vguser",
            password="vgpwd",
            database="vgdb",
            pool_size=7
        )
        curs = mysqlconnection.cursor(dictionary=True)
        curs.execute(sql)
        mysqlconnection.commit()
        #print("Executed : {}".format(sql))
        return curs
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_BAD_DB_ERROR:
            print('ERROR : Database does not exist. Please verify connection string.')
        elif error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('ERROR : Please verify your credentials to connect to the database')
        else:
            print("ERROR: Cannot connect to mysql : {} ".format(error))
    except TypeError as e:
        print(e)
        return None
    except ValueError as e:
        print(e)
        return None
    finally:
        if mysqlconnection.is_connected():
            curs.close()
            mysqlconnection.close()
            # print("MySQL Connection is closed")

def qryMySQLDB(sql):
    try:
        """
        mysqlconnection = mysql.connector.connect(
            host="localhost",
            user="vguser",
            password="vgpwd",
            database="vgdb",
            pool_size=7
        )
        sql_qry = pd.read_sql_query(sql, mysqlconnection)
        df = pd.DataFrame(sql_qry)
        """
        sql_qry = pd.read_sql_query(sql, con=engine)
        df = pd.DataFrame(sql_qry)
        return df
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_BAD_DB_ERROR:
            print('ERROR : Database does not exist. Please verify connection string.')
        elif error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('ERROR : Please verify your credentials to connect to the database')
        elif error.errno == errorcode.ER_DUP_INDEX:
            print('WARNING : DUP VALUE ON INDEX')
        else:
            print("ERROR: Cannot connect to mysql : {} ".format(error))
    except TypeError as e:
        print(e)
        return None
    except ValueError as e:
        print(e)
        return None

# insert SYMBOLS into mysql table
def storeSymbols():
    print("INFO: About to store : {}".format(len(lstSyms)))
    lstSyms.sort()
    for sym in lstSyms:
        ydata = yf.Ticker(sym)
        if "sector" in ydata.info:
            ysector = ydata.info['sector']
        else:
            ysector = "N/A"
        if "volume" in ydata.info:
            yvolume = ydata.info['volume']
        else:
            yvolume = 0
        if "marketCap" in ydata.info:
            ymktcap = ydata.info['marketCap']
        else:
            ymarketcap = 0
        if "quoteType" in ydata.info:
            yquotetype = ydata.info['quoteType']
        else:
            yquotetype = "N/A"
        insert_query = "INSERT INTO symbols (SYMBOL, SECTOR, VOLUME, MARKETCAP, QUOTETYPE) VALUES ( '" + sym + "','" + str(ysector) + "'," + str(yvolume) + "," + str(ymarketcap) + ",'" + str(yquotetype) + "' ) "
        print(insert_query)
        dmlMySQLDB(insert_query)
        if sym == "AAPL":
            break

def getYfinanceData(psym, penddate):
    symdata = yf.Ticker(psym)
    # enddt = row[0].strftime('%Y-%m-%d')
    # print("Date:{}".format(enddt))
    # get stock info
    # sym.info
    # get historical market data
    hist = symdata.history(start="2018-01-01", end=penddate, interval="1d")
    return hist


def storeYdata():
    enddate = datetime.now().strftime('%Y-%m-%d')
    for sym in lstSyms:
        dfhist = getYfinanceData(sym, enddate)
        if len(dfhist) > 0:
            #print(dfhist)
            dfhist = dfhist.reset_index()
            for i in range(0, len(dfhist)):
                #print("ROWDATA: sym:{}; i:{} ; date:{} ; col0:{} ; col1:{}".format(sym, i, dfhist.iloc[i]['Date'],
                #                                                                   dfhist.iloc[i]['Open'],
                #                                                                   dfhist.iloc[i]['High']))
                # sql = "INSERT INTO SYMHISTORY (SYMBOL, HISTDATE, OPENPRICE) values ('" + sym + "," + str(dfhist.iloc[i]['Date']) + "," + str(dfhist.iloc[i]['Open']) + ")"
                sql = "INSERT INTO SYMHISTORY (SYMBOL, HISTDATE, OPENPRICE, HIGHPRICE, LOWPRICE, CLOSEPRICE, VOLUME, DIVIDENDS, STOCKSPLITS) VALUES " \
                      " ( '" + sym + "','" + str(dfhist.iloc[i]['Date']) + "'," + str(
                    dfhist.iloc[i]['Open']) + "," + str(dfhist.iloc[i]['High']) + "," + \
                      str(dfhist.iloc[i]['Low']) + "," + str(dfhist.iloc[i]['Close']) + "," + str(
                    dfhist.iloc[i]['Volume']) + "," + \
                      str(dfhist.iloc[i]['Dividends']) + "," + str(dfhist.iloc[i]['Stock Splits']) + " )"
                #print(sql)
                dmlMySQLDB(sql)
        if sym == "AAPL":
            break

def calcSectorIndex():
    i=0
    sql = "SELECT distinct symbol, sector FROM SYMBOLS"
    df = qryMySQLDB(sql)
    print(df)
    #for row in df.iterrows():


if __name__ == '__main__':
    #processPickleFile()
    #storeSymbols()
    #storeYdata()
    calcSectorIndex()
