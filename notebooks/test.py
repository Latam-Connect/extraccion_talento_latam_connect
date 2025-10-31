import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    sslmode=os.getenv("PG_SSLMODE")
)

def show_columns(table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        print(f"\n📋 {table.upper()}")
        for name, dtype in cur.fetchall():
            print(f" - {name} ({dtype})")

for t in ["profiles", "experiences", "educations", "profile_skills", "profile_languages", "languages", "skills"]:
    show_columns(t)

conn.close()
