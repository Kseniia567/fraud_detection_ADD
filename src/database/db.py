import psycopg2
import os
from psycopg2 import OperationalError

def connect():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            database=os.environ.get("POSTGRES_DB", "fraud_db"),
            user=os.environ.get("POSTGRES_USER", "user"),
            password=os.environ.get("POSTGRES_PASSWORD", "password"),
            port=os.environ.get("POSTGRES_PORT", 5432)
        )

        return conn
    except OperationalError as e:
        print("Error connecting to the database:", e)
        return None