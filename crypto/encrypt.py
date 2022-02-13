from cryptography.fernet import Fernet
key = Fernet.generate_key()
f = Fernet(key)
token = f.encrypt(b"vgpwd")
print(key)
print(token)

open("crypto_fernet.key", "wb").write(key)
open("crypto_fernet.token", "wb").write(token)

nkey = open("crypto_fernet.key", "rb").read()
ntoken = open("crypto_fernet.token", "rb").read()

print("key {}".format(nkey))
print("token {}".format(ntoken))
fn = Fernet(nkey)
print(fn.decrypt(ntoken).decode("utf-8") )

