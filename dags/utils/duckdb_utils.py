from airflow.models import Variable
import psycopg2
import duckdb



def sync_table_to_duckdb(table_name: str):
    pg_conn = psycopg2.connect(Variable.get('DB_CONNECTION'))
    cursor = pg_conn.cursor()
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    pg_conn.close()

    con = duckdb.connect('/opt/airflow/data/football.duckdb')
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(f"{c} VARCHAR" for c in columns)})
    ''')
    con.execute(f'DELETE FROM {table_name}')
    con.executemany(
        f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(columns))})",
        rows
    )
    con.close()

