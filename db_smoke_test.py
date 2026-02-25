import os
import psycopg2

host = os.getenv("PGHOST", "localhost")
port = int(os.getenv("PGPORT", "5433"))
dbname = os.getenv("PGDATABASE", "ups_billing")
user = os.getenv("PGUSER", "ups_user")
password = os.getenv("PGPASSWORD", "ups_password")

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
