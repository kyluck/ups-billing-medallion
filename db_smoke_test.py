import os
import psycopg2

host = os.getenv("POSTGRES_HOST", "localhost")
port = int(os.getenv("POSTGRES_PORT", "5433"))
dbname = os.getenv("POSTGRES_DB", "ups_billing")
user = os.getenv("POSTGRES_USER", "ups_user")
password = os.getenv("POSTGRES_PASSWORD", "ups_password")

print("Connecting with:")
print(" host =", host)
print(" port =", port)
print(" db   =", dbname)
print(" user =", user)

conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password,
)

cur = conn.cursor()
cur.execute("select current_user, current_database();")
print("Result:", cur.fetchone())

cur.close()
conn.close()
print("OK")

