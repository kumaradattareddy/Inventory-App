import secrets, hashlib
salt = secrets.token_hex(16)
pwd = "1234"
hash_hex = hashlib.pbkdf2_hmac("sha256", pwd.encode(), bytes.fromhex(salt), 100_000).hex()
print(salt, hash_hex)
