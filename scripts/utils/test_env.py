from dotenv import load_dotenv
import os

load_dotenv()
print("POSTGRES_HOST:", os.getenv("POSTGRES_HOST"))
print("POSTGRES_DB:", os.getenv("POSTGRES_DB"))
print("POSTGRES_SCHEMA:", os.getenv("POSTGRES_SCHEMA"))
print("MINIO_ENDPOINT:", os.getenv("MINIO_ENDPOINT"))
print("MINIO_BUCKET:", os.getenv("MINIO_BUCKET"))