import os
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
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d

import loadrefdata
import utils
from utils import *
import initvar
import subprocess
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")

#TODO : replace with dbconnect.configconnection
def configconnection():
    # Files used : config\vg.config
    # Uses this configuration settings to create mysql connection
    # Uses sqlalchemy package and create_engine modules
    # sqlalchemy is performant than using mysql-connector
    # sets global variables for connections

    global connection_string
    global engine
    global mysqluser
    global mysqldb
    global mysqlhost

    print('CREATING CONNECTION TO MYSQL DATABASE ')

    config = configparser.ConfigParser()
    config.sections()
    config.read('config\\vg.config')
    config.sections()

    decodedpwd = utils.getuserpwd()

    connection_string = "mysql+pymysql://%s:%s@%s/%s" % (mysqluser, decodedpwd, mysqlhost, mysqldb)
    engine = create_engine(connection_string)


def populateSyms(pListsyms):
    # populate global list to ensure we have the universal symbols list
    # global lstSyms

    for sym in pListsyms:
        if sym == '-':
            continue
        if not lstSyms:
            lstSyms.append(sym)
        elif sym not in lstSyms:
            lstSyms.append(sym)


def processPickleFile():
    global appsymbolslist

    print(' *** PROCESSING PICKLE FILE - TO GET ALL CONSTITUENTS *** ')

    pickle_inputfile = os.path.abspath("data\\constituents_history.pkl")
    pickle_file = open(pickle_inputfile, "rb")
    unpickled = pickle.load(pickle_file)

    if len(appsymbolslist) == 1 and appsymbolslist[0] == "ALL":
        for row in unpickled.itertuples(name='SymData'):
            # row[0] is date
            # row[1] is symbol constituent data
            # extract symbol from this list by using itemgetter - first column in list, so index is 0
            symDataList = row[1]
            if symDataList:
                l_listsymbols = list(map(itemgetter(0), symDataList))
                populateSyms(l_listsymbols)
                # symAssetClass=list(map(itemgetter(3), symDataList))
                # print("Row:{} ; Date:{}; Symbols:{}; AssetClass :{}".format( r, row[0], syms, symAssetClass ) )
    else:
        populateSyms(appsymbolslist)


def dmlMySQLDB(sql):
    # Parameters
    # sql: SQL query to be executed

    # Returns
    # sql result as cursor to the caller

    global mysqluser
    global mysqldb
    global mysqlhost
    global mysqlpoolsize

    decodepwd = utils.getuserpwd()

    # print (mysqluser)

    try:
        mysqlconnection = mysql.connector.connect(
            host=mysqlhost,
            user=mysqluser,
            password=decodepwd,
            database=mysqldb,
            pool_size=mysqlpoolsize
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
        elif error.errno == errorcode.ER_DUP_INDEX:
            print('WARNING : DUP VALUE ON INDEX')
        elif error.errno == errorcode.ER_DUP_ENTRY:
            print('WARNING : DUP VALUE ON KEY')
        else:
            print("ERROR: Other mysql : {} ".format(error))
    except TypeError as e:
        print(e)
        return None
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        print(e)
        return None
    finally:
        if mysqlconnection.is_connected():
            curs.close()
            mysqlconnection.close()
            # print("MySQL Connection is closed")


def qryMySQLDB(sql):
    # Parameters
    # sql: SQL query to be executed

    # Returns
    # sql result set as dataframe object

    # global engine
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
            print("ERROR: Other mysql : {} ".format(error))
    except TypeError as e:
        print(e)
        return None
    except ValueError as e:
        print(e)
        return None
    except Exception as e:
        return None


def storeSymbols():
    # Stores data into SYMBOLS table
    # Fetches symbol info from yfinance (sym.info) and stores in table SYMBOLS
    # This symbols info data can later be used for many computations

    print(" *** STORING SYMBOLS - about to store symbols : {} *** ".format(len(lstSyms)))
    lstSyms.sort()

    for sym in lstSyms:

        # TODO : Doing this one at a time slows down. Need to implement batch ticker list querying to yfinance
        #        Takes about a minute for each symbol
        #        Tried to do in batch as well
        #        TEMPORARY SOLUTION : Downloaded Symbol sector information from yahoo finance and using this as a reference data file
        #        This is done one time using refdata.sh script
        #        We use this table to populate sector information instead

        #ydata = yf.Ticker(sym)

        # if "sector" in ydata.info:
        #     ysector = ydata.info['sector']
        # else:
        #     ysector = "N/A"
        # if "volume" in ydata.info:
        #     yvolume = ydata.info['volume']
        # else:
        #     yvolume = 0
        # if "marketCap" in ydata.info:
        #     ymarketcap = ydata.info['marketCap']
        # else:
        #     ymarketcap = 0
        # if "quoteType" in ydata.info:
        #     yquotetype = ydata.info['quoteType']
        # else:
        #     yquotetype = "N/A"

        #insert_query = "INSERT INTO symbols (SYMBOL, SECTOR, VOLUME, MARKETCAP, QUOTETYPE) VALUES ( '" + sym + "','" + ysector + "'," + str(yvolume) + "," + str(ymarketcap) + ",'" + str(yquotetype) + "' ) "

        insert_query = "INSERT INTO symbols (SYMBOL, SECTOR, NAME, EXCHANGE) SELECT ticker, Category, CompanyName, Exchange FROM refdata WHERE ticker = '" + sym + "' "
        # print(insert_query)
        dmlMySQLDB(insert_query)


def getYfinanceData(psym, penddate):
    # Parameters:
    # psym : Symbol
    # penddate : end date for history

    # Returns:
    # Dataframe with required history for symbol

    # Uses YFinance API sym.history to get data between start and end date by interval.
    # Interval is fetched from application configuration

    global startdate

    symdata = yf.Ticker(psym)
    # get historical market data
    hist = symdata.history(start=startdate, end=penddate, interval=interval)
    return hist


def storeYHistorydata():
    # Uses yfinance API to get historical data for symbol and store this data in SYMHISTORY table
    # Yfinance API tends to be slower, batch processing of symbols can be done if needed

    global lstSyms
    print(' *** FETCHING YFinance Data and STORING IN DATABASE *** ')

    enddate = datetime.now().strftime('%Y-%m-%d')

    if not lstSyms:
        qry = " SELECT DISTINCT SYMBOL FROM SYMBOLS ORDER BY 1"
        df = qryMySQLDB(qry)
        lstSymsN = df['SYMBOL'].unique()
    else:
        lstSymsN = lstSyms

    # TODO : Doing this one at a time slows down. Need to implement batch ticker list querying to yfinance
    #        Will raise symbols are delisted : No data found, symbol may be delisted
    for sym in lstSymsN:
        if sym == stopsymbol:
            break
        dfhist = getYfinanceData(sym, enddate)
        if len(dfhist) > 0:
            # print(dfhist)
            dfhist = dfhist.reset_index()
            for i in range(0, len(dfhist)):
                # print(sql)

                # skip bad data from Yfinance. some cases openprice is coming in as NaN. e.g. AAPL 2018-02-09
                if dfhist.iloc[i]['Open'] > 0:
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
    print ('CALCULATING SECTOR INDEX')

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
    # Performs DDL operations to cleanup tables
    # Please make sure user has DDL permissions

    global freshrun

    if freshrun == "Y":
        print(' *** PERFORMING CLEANUP *** ')
        dsql = "truncate table vgdb.symhistory"
        dmlMySQLDB(dsql)
        dsql = "truncate table vgdb.symbols"
        dmlMySQLDB(dsql)
        dsql = "truncate table vgdb.sectorweight"
        dmlMySQLDB(dsql)


def setApplicationConfig():
    # Reads configuration from config\vg.config file
    # Sets configuration for application

    global startdate
    global stopsymbol
    global chksymbol
    global interval

    global mysqluser
    global mysqldb
    global mysqlhost
    global mysqlpoolsize
    global mysqlexepath
    global appsymbolslist
    global lstrootcommand
    global mysqlrootpwd
    global freshrun
    global lstSyms

    print(' *** SETTING APPLICATION CONFIG *** ')

    config = configparser.ConfigParser()
    config.sections()
    config.read('config\\vg.config')
    config.sections()

    # store application configurations
    startdate = config['APPLICATION']['STARTDATE'].strip()
    stopsymbol = config['APPLICATION']['STOPSYMBOL'].strip()
    chksymbol = config['APPLICATION']['CHKSYMBOL'].strip()
    interval = config['APPLICATION']['INTERVAL'].strip()
    appsymbolslist = config['APPLICATION']['SYMBOLSLIST'].strip().split(',')
    freshrun = config['APPLICATION']['FRESHRUN'].strip()
    lstSyms = []

    # store mysql database configurations
    mysqluser = config['MYSQL']['MYSQLUSER'].strip()
    mysqldb = config['MYSQL']['MYSQLDATABASE'].strip()
    mysqlhost = config['MYSQL']['MYSQLHOST'].strip()
    mysqlexepath = config['MYSQL']['MYSQLEXEPATH'].strip()
    mysqlpoolsize = int(config['MYSQL']['MYSQLPOOLSIZE'])
    mysqlrootpwd = utils.getrootpwd()
    rootpwdcmd = "-p" + mysqlrootpwd
    lstrootcommand = [mysqlexepath, "-uroot", rootpwdcmd]

    # set dataframe to print all columns
    pd.set_option('display.max_columns', None)


def createDatabase():
    #Creates new database
    #If database exists will skip creating new database

    global mysqldb
    global mysqlhost
    global lstrootcommand
    global mysqlrootpwd

    print(' *** CREATING DATABASE *** ')

    try:
        mysqlconnection = mysql.connector.connect(
            host=mysqlhost,
            user="root",
            password=mysqlrootpwd
        )
        curs = mysqlconnection.cursor(dictionary=True)
        curs.execute("SHOW DATABASES")

        for x in curs:
            if x["Database"] == mysqldb:
                print("Skipping creating database '{}' as it already exists".format(mysqldb))
                return

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
    except Exception as e:
        return None
    finally:
        if mysqlconnection.is_connected():
            curs.close()
            mysqlconnection.close()

    with open('sqlscripts/createDatabase.sql') as input_file:
        result = subprocess.run(lstrootcommand, stdin=input_file, capture_output=True)
        print(result)


def createUser():
    #Creates new user
    #If user exists will skip creating new user

    global mysqluser
    global mysqlhost
    global lstrootcommand
    global mysqlrootpwd

    print(' *** CREATING USER *** ')

    try:
        mysqlconnection = mysql.connector.connect(
            host=mysqlhost,
            user="root",
            password=mysqlrootpwd
        )
        mysqlconnection.raise_on_warnings = True
        curs = mysqlconnection.cursor(dictionary=True)
        curs.execute("select user from mysql.user")

        for x in curs:
            if x["user"] == mysqluser:
                print("Skipping creating user '{}' as it already exists".format(mysqluser))
                return
    except mysql.Warning as w:
        i=0
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
    except Exception as e:
        return None
    finally:
        if mysqlconnection.is_connected():
            curs.close()
            mysqlconnection.close()

    with open('sqlscripts/createUser.sql') as input_file:
        result = subprocess.run(lstrootcommand, stdin=input_file, capture_output=True)
        print(result)


def createTables():
    #Creates new tables required for this project
    #If tables exists will skip creating tables

    global mysqldb
    global mysqlhost
    global mysqlexepath
    global lstrootcommand

    print(' *** CREATING TABLES *** ')

    with open('sqlscripts/createTables.sql') as input_file:
        result = subprocess.run(lstrootcommand, stdin=input_file, capture_output=True)
        print(result)


def displayAggregates():
    # Shows aggregates for all tables
    # Quick view of data

    print(' *** DISPLAY AGGREGATES *** ')

    printLineSeparator()
    dsql = "select 'SYMBOLS' as TableName, count(*) as NumberOfRows  from vgdb.symbols"
    df = qryMySQLDB(dsql)
    print(df)

    printLineSeparator()
    dsql = "select 'SYMHISTORY' as TableName, count(*) as NumberOfRows from vgdb.symhistory"
    df = qryMySQLDB(dsql)
    print(df)

    printLineSeparator()
    dsql = "select 'SECTORWEIGHT' as TableName, count(*) as NumberOfRows  from vgdb.sectorweight"
    df = qryMySQLDB(dsql)
    print(df)
    printLineSeparator()


def sample2dgraph():
    print(' *** SHOW 2D GRAPH *** ')

    # , SECTORWINDEX, TOTALCONSTITUENTS
    sql = " SELECT DATE, COUNT(SECTORNAME) AS SECTORNAME, SUM(SECTORWINDEX)  as SECTORWINDEX, SUM(TOTALCONSTITUENTS) AS TOTALCONSTITUENTS " \
          " FROM SECTORWEIGHT " \
          " GROUP BY DATE ORDER BY 1 DESC"

    df = qryMySQLDB(sql)
    df = df.reset_index()

    lstDt = df['DATE'].tolist()
    lstSecWIndex = df['SECTORWINDEX'].tolist()

    plt.plot(lstDt, lstSecWIndex, label="Sector Weighted Index")
    #plt.plot(lstDt, lstSec, label="Sectors")
    #plt.plot(lstDt, lstConstituents, label="Total Constituents")

    plt.xlabel('x - date')
    plt.ylabel('y - sector weighted index')
    plt.legend()
    plt.show()


def sampleSymgraph():
    print(' *** SHOW Sample Symbol GRAPH *** ')
    global chksymbol
    global startdate

    sql = " SELECT SYMBOL, HISTDATE, ROUND(OPENPRICE,2) as OPENPRICE, ROUND(CLOSEPRICE,2) as CLOSEPRICE, " \
          " ROUND(HIGHPRICE,2) as HIGHPRICE, ROUND(LOWPRICE,2) AS LOWPRICE, VOLUME " \
          " FROM symhistory " \
          " WHERE symbol = '" + chksymbol + "' and histdate > '" + startdate + "'"

    df = qryMySQLDB(sql)
    df = df.reset_index()

    lstDt = df['HISTDATE'].tolist()
    lstOP = df['OPENPRICE'].tolist()
    plt.plot(lstDt, lstOP, label="Open Price")

    lstCP = df['CLOSEPRICE'].to_list()
    plt.plot(lstDt, lstCP, label="Close Price")

    lstCP = df['HIGHPRICE'].to_list()
    plt.plot(lstDt, lstCP, label="High Price")

    lstCP = df['LOWPRICE'].to_list()
    plt.plot(lstDt, lstCP, label="Low Price")

    plt.xlabel('x - Date')
    plt.ylabel('y - Price')

    plt.legend()
    plt.show()


def sample3dgraph():
    print(' *** SHOW 3D GRAPH *** ')

    sql = " SELECT SECTORNAME as DATE, TOTALCONSTITUENTS, SECTORWINDEX " \
          " FROM SECTORWEIGHT " \
          " ORDER BY 1 DESC"

    df = qryMySQLDB(sql)
    # df = df.reset_index()

    lstDt = df['DATE'].tolist()
    lstSec = df['TOTALCONSTITUENTS'].tolist()
    lstSecWIndex = df['SECTORWINDEX'].tolist()

    fig = plt.figure()

    # set 3-D projection
    ax = plt.axes(projection='3d')

    # define all 3 axes
    yn = range(len(lstSec))
    y = yn
    xn = range(len(lstDt))
    x = xn
    z = lstSecWIndex

    # plott axis
    ax.plot3D(x, y, z, 'blue')
    ax.set_title('3D Date vs Sector vs Index')
    plt.show()


if __name__ == '__main__':

    # Loads static reference data. Replaces existing refdata
    loadrefdata.main()

    # Loads application configuration - uses config/vg.config file
    setApplicationConfig()

    # Creates Database - during first run, ignores subsequent runs
    createDatabase()

    # Creates User - during first run, ignores subsequent runs
    createUser()

    # Creates Tables - during first run, ignores subsequent runs
    createTables()

    # creates Mysql Connection using sqlalchemy
    configconnection()

    # cleans up tables, if FRESHRUN=Y in config/vg.config
    cleanup()

    # loads constituents from pickle file. Given pickle file is stored in data/constituents_history.pkl file
    processPickleFile()

    # stores symbol information for each constituent. It uses yfinance.info file.
    # However for performance reasons I have loaded data from static reference data.
    storeSymbols()

    # Stores History data , pulled from yfinance module.
    # startdate, interval, stop symbol etc are all properties used from config/vg.config file
    storeYHistorydata()

    # Calculates sector weighted Index. In the same way as used for DOW Price weighted Index
    calcSectorIndex()

    # Displays stats on all tables loaded
    displayAggregates()

    # Displays sample 2d graph
    sample2dgraph()

    # Displays sample 3d graph
    sample3dgraph()

    # Displays multi plot graph
    # Uses symbol from chksymbol property defined in config/vg.config file
    sampleSymgraph()

    exit()
