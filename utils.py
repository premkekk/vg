from cryptography.fernet import Fernet

def getcrypto():
    nkey = open("crypto\\crypto_fernet.key", "rb").read()
    ntoken = open("crypto\\crypto_fernet.token", "rb").read()

    print("key {}".format(nkey))
    print("token {}".format(ntoken))
    fn = Fernet(nkey)
    return fn.decrypt(ntoken).decode("utf-8")

