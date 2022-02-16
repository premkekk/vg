from cryptography.fernet import Fernet
import configparser
from sqlalchemy import create_engine

connection_string = ""
engine = ""

def getcrypto():
    nkey = open("crypto\\crypto_fernet.key", "rb").read()
    ntoken = open("crypto\\crypto_fernet.token", "rb").read()

    #print("key {}".format(nkey))
    #print("token {}".format(ntoken))
    fn = Fernet(nkey)
    return fn.decrypt(ntoken).decode("utf-8")

def configconnection():
    global connection_string
    global engine

    config = configparser.ConfigParser()
    config.sections()
    config.read('config\\vg.config')
    config.sections()

    mysqluser = config['MYSQL']['MYSQLUSER']
    mysqldb = config['MYSQL']['MYSQLDATABASE']
    mysqlhost = config['MYSQL']['MYSQLHOST']

    # call get crypto
    decodedpwd = getcrypto()

    #print("decrypted string: ", decodedpwd)

    connection_string = "mysql+pymysql://%s:%s@%s/%s" % (mysqluser, decodedpwd, mysqlhost, mysqldb)
    engine = create_engine(connection_string)

    #print(connection_string)
    #print(engine)

