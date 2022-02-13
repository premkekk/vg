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
import sqlalchemy.util

import configparser
from collections import OrderedDict
import numpy as np
from matplotlib import pyplot as plt
from utils import *

lstSyms = []
connection_string = ""
engine = ""
startdate = ""

def configconnection():
    # Files used : config\vg.config
    # Uses this configuration settings to create mysql connection
    # Uses sqlalchemy package and create_engine modules
    # sqlalchemy is performant than using mysql-connector
    # sets global variables for connections

    global connection_string
    global engine

    config = configparser.ConfigParser()
    config.sections()
    config.read('config\\vg.config')
    config.sections()

    mysqluser = config['MYSQL']['MYSQLUSER']
    mysqldb = config['MYSQL']['MYSQLDATABASE']
    mysqlhost = config['MYSQL']['MYSQLHOST']
    decodedpwd = getcrypto()

    connection_string = "mysql+pymysql://%s:%s@%s/%s" % (mysqluser, decodedpwd, mysqlhost, mysqldb)
    engine = create_engine(connection_string)
    #print(connection_string)
    #print(engine)

def populateSyms(pListsyms):
    # populate global list to ensure we have the universal symbols list
    global lstSyms

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
        # print("Executed : {}".format(sql))
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
    global engine
    try:
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
        insert_query = "INSERT INTO symbols (SYMBOL, SECTOR, VOLUME, MARKETCAP, QUOTETYPE) VALUES ( '" + sym + "','" + str(
            ysector) + "'," + str(yvolume) + "," + str(ymarketcap) + ",'" + str(yquotetype) + "' ) "
        print(insert_query)
        dmlMySQLDB(insert_query)
        if sym == "CSO":
            break


def getYfinanceData(psym, penddate):
    #Parameters:
    #psym : Symbol
    #pstartdate : start date for history
    #penddate : end date for history

    #Returns:
    #    dataframe with required history for symbol

    global startdate

    symdata = yf.Ticker(psym)
    # enddt = row[0].strftime('%Y-%m-%d')
    # print("Date:{}".format(enddt))
    # get stock info
    # sym.info
    # get historical market data
    hist = symdata.history(start=startdate, end=penddate, interval="1d")
    return hist

def storeYdata():
    #Uses yfinance API to get historical data for symbol and store this data in SYMHISTORY table
    #Yfinance API tends to be slower, batch processing of symbols can be done if needed

    global lstSyms

    enddate = datetime.now().strftime('%Y-%m-%d')
    if not lstSyms:
        qry = " SELECT DISTINCT SYMBOL FROM SYMBOLS ORDER BY 1"
        df = qryMySQLDB(qry)
        lstSymsN = df['SYMBOL'].unique()
    else:
        lstSymsN = lstSyms
    for sym in lstSymsN:
        #if sym == "AAL":
        #    break
        dfhist = getYfinanceData(sym, enddate)
        if len(dfhist) > 0:
            # print(dfhist)
            dfhist = dfhist.reset_index()
            for i in range(0, len(dfhist)):
                # print(sql)
                sql = "INSERT INTO SYMHISTORY (SYMBOL, HISTDATE, OPENPRICE, HIGHPRICE, LOWPRICE, CLOSEPRICE, VOLUME, DIVIDENDS, STOCKSPLITS) VALUES " \
                      " ( '" + sym + "','" + str(dfhist.iloc[i]['Date']) + "'," + \
                      str(dfhist.iloc[i]['Open']) + "," + str(dfhist.iloc[i]['High']) + "," + \
                      str(dfhist.iloc[i]['Low']) + "," + str(dfhist.iloc[i]['Close']) + "," + \
                      str(dfhist.iloc[i]['Volume']) + "," + \
                      str(dfhist.iloc[i]['Dividends']) + "," + str(dfhist.iloc[i]['Stock Splits']) + " )"
                dmlMySQLDB(sql)


def calcSectorIndex():
    # symhistory is the date wise yfinance data
    # symbols is the symbols loaded from constituents file along with sector information loaded from yfinance
    # Symbols table has symbol and sector information. We will use this to drive the sector calculation for performance improvements
    # idea is symbols table will be used as a driver to calculate sector weighted index
    # Doing equi join between symbols and symhistory tables.

    # First cleanup sector weight index table
    dsql = "truncate table vgdb.sectorweight"
    dmlMySQLDB(dsql)

    # Create dataframe to store symbol to sector mapping
    sql = " SELECT distinct symbol, sector FROM SYMBOLS " \
          " WHERE SECTOR != 'N/A'"
    df = qryMySQLDB(sql)
    # print(df)

    # Calculate sector price across all symbols from driver table and use this to calculate sector weight index
    # This will then be inserted into the sectorweight table
    # As this is a price weighted index, stock split are not handled. If this needs to be changed then it can be done by using stock split field from symhistory table as well
    # Grouping across dates
    # Note : Database queries would be more performant if date wise partition is created on symhistory table.
    for sector in df['sector'].unique():
        """
        sql = " SELECT h.HISTDATE, s.SECTOR, count(h.symbol) as TOTALSYMBOLS, SUM(h.CLOSEPRICE) as TOTALCLOSEPRICE,  SUM(h.CLOSEPRICE)/count(h.symbol) as SECTORINDEXVALUE" \
              " FROM vgdb.symbols s, vgdb.symhistory h" \
              " WHERE s.symbol = h.symbol" \
              " AND s.sector = '" + sector + "'" \
              " GROUP BY h.HISTDATE, s.SECTOR"
        sdf = qryMySQLDB(sql)
        print(sdf)
        """

        isql = " INSERT INTO SECTORWEIGHT (DATE, SECTORNAME, TOTALCONSTITUENTS, SECTORPRICE, SECTORWINDEX) " \
               " SELECT h.HISTDATE, s.SECTOR, count(h.symbol) as TOTALSYMBOLS, SUM(h.CLOSEPRICE) as TOTALCLOSEPRICE,  SUM(h.CLOSEPRICE)/count(h.symbol) as SECTORINDEXVALUE" \
               " FROM vgdb.symbols s, vgdb.symhistory h" \
               " WHERE s.symbol = h.symbol" \
               " AND s.sector = '" + sector + "'" \
               " GROUP BY h.HISTDATE, s.SECTOR"
        dmlMySQLDB(isql)


def cleanup():
    #Performs DDL operations to cleanup tables
    #Please make sure user has DDL permissions

    dsql = "truncate table vgdb.symhistory"
    dmlMySQLDB(dsql)
    dsql = "truncate table vgdb.symbols"
    dmlMySQLDB(dsql)
    dsql = "truncate table vgdb.sectorweight"
    dmlMySQLDB(dsql)

def showaggregates():
    #Shows aggregates for all tables
    #Quick view of data

    dsql = "select count(*) from vgdb.symhistory"
    df = qryMySQLDB(dsql)
    print(df)
    dsql = "select count(*) from vgdb.symbols"
    df = qryMySQLDB(dsql)
    print(df)
    dsql = "select count(*) from vgdb.sectorweight"
    df = qryMySQLDB(dsql)
    print(df)

def setapplicationconfig():
    global startdate

    config = configparser.ConfigParser()
    config.sections()
    config.read('config\\vg.config')
    config.sections()

    startdate = config['APPLICATION']['STARTDATE']

def chkYfinance():
    enddate = datetime.now().strftime('%Y-%m-%d')
    dfhist = getYfinanceData("CSCO", enddate)
    print(dfhist)

if __name__ == '__main__':
    setapplicationconfig()
    configconnection()
    chkYfinance()
    #cleanup()
    showaggregates()
    #processPickleFile()
    #storeSymbols()
    #storeYdata()
    #calcSectorIndex()

