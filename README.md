# VG YFinance Code

<table border=1 cellpadding=10>
<tr>
<td>
** Developed using **
Pycharm - virtual environment
MySQL - mysql workbench
</td>
</tr>
</table>

---

## Quick Start

### Set virtual environment for python
### Packages to be installed are in requirements.txt. There are more packages than required but left them for up-scoping purposes.
### This program makes use of MySQL database. Please install this.
### For security making using of encryption for passwords
### Please refer to the create user script for create user details. The encryption files are created with this pwd.

```python
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
    

```


```python
    # Project Structure  
    
    # config folder
    # holds application configuration
    
    # crypto folder
    # stores keys for root and vguser mysql users    

    # data folder 
    # holds all sample and constituents data and reference data
    
    # docs folder
    # stores pdf of this project requirements - received from Mr. Jose Arguelles
    # stores yfinance documentation
    # stores test results
    
    # sqlscripts folder
    # holds all sql scripts - mainly DDL queries
    
    # helper programs
    # dbconnect.py --- to connect to mysql
    # loadrefdata.py --- to load reference data
    # initvar.py --- holds global variable initialization
    # RunYFinance.py --- program to test yfinance module
    # testencryption.py --- to test encryption
    # utils.py --- for helper utils
    # main.py --- main program for this project

```

