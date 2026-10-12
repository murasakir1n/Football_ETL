import time
from airflow.models import Variable
import psycopg2
import duckdb



def sync_table_to_duckdb(table_name: str, max_retries: int = 5, retry_delay: int = 10):
    pg_conn = psycopg2.connect(Variable.get('DB_CONNECTION'))
    cursor = pg_conn.cursor()
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    pg_conn.close()

    for attempt in range(max_retries):
        try:
            con = duckdb.connect('/opt/airflow/data/football.duckdb')
            break
        except duckdb.IOException:
            if attempt == max_retries -1 :
                raise
            time.sleep(retry_delay)

    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(f"{c} VARCHAR" for c in columns)})
    ''')
    con.execute(f'DELETE FROM {table_name}')

    if rows:
        con.executemany(
            f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(columns))})",
            rows
        )
    con.close()

