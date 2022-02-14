import os
import yfinance as yf
from datetime import datetime
from utils import *
import initvar
import configparser
import pandas as pd

def getYfinanceData(psym, penddate):
    #Parameters:
    #psym : Symbol
    #penddate : end date for history

    #Returns:
    # Dataframe with required history for symbol

    # Uses YFinance API sym.history to get data between start and end date by interval.
    # Interval is fetched from application configuration

    global startdate
    global interval

    symdata = yf.Ticker(psym)
    # get historical market data
    hist = symdata.history(start=startdate, end=penddate, interval=interval)
    return hist


def chkYfinance():
    #Function to check yfinance data
    #start date is from configuration file vg.config - property ['APPLICATION']['STARTDATE']
    #symbol is from configuration file vg.config - property ['APPLICATION']['CHKSYMBOL']
    #interval is from configuration file vg.config - property ['APPLICATION']['INTERVAL']
    #end date is set to current date

    global chksymbol

    enddate = datetime.now().strftime('%Y-%m-%d')
    dfhist = getYfinanceData(chksymbol, enddate)

    printLineSeparator()
    print("Check YFinance data for {}".format(chksymbol))
    printLineSeparator()
    pd.set_option('display.max_columns', 8)
    print(dfhist)
    printLineSeparator()


def setApplicationConfig():
    #Reads configuration from config\vg.config file
    #Sets configuration for application

    global chksymbol
    global interval
    global startdate

    config = configparser.ConfigParser()
    config.sections()
    config.read('config\\vg.config')
    config.sections()

    #store application configurations
    startdate = config['APPLICATION']['STARTDATE']
    chksymbol = config['APPLICATION']['CHKSYMBOL']
    interval = config['APPLICATION']['INTERVAL']

    #set to 8 columns to allow to print all columns
    pd.set_option('display.max_columns', None)

if __name__ == '__main__':
    setApplicationConfig()
    chkYfinance()
