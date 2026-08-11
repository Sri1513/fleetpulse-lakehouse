from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
schema = os.getenv("POSTGRES_SCHEMA")

conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=database,
    user=user,
    password=password
)

cursor = conn.cursor()


cursor.execute("""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = %s
ORDER BY table_name;
""", (schema,))

tables = cursor.fetchall()
print("Connected to PostgreSQL successfully.")
print(f"Tables found in schema '{schema}':")

for table in tables:
    print("-", table[0])

cursor.close()
conn.close()