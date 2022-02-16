from cryptography.fernet import Fernet

#generate key
key = Fernet.generate_key()
f = Fernet(key)
#encrypt message and generate token message
#create token files for root and mysql user "vguser"
token = f.encrypt(b"root")
print(key)
print(token)

# open key and token files for writing
open("crypto_fernet.root.key", "wb").write(key)
open("crypto_fernet.root.token", "wb").write(token)

# read key and token back
nkey = open("crypto_fernet.root.key", "rb").read()
ntoken = open("crypto_fernet.root.token", "rb").read()

print("key {}".format(nkey))
print("token {}".format(ntoken))

# create fernet object using new key
fn = Fernet(nkey)

#decrypt new token and convert to utf-8 format
print(fn.decrypt(ntoken).decode("utf-8"))

