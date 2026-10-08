from datetime import datetime, timedelta
import json
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
import boto3
from sqlalchemy import create_engine, text
from datetime import date, timedelta
import psycopg2
import clickhouse_connect

from sqlalchemy.dialects.postgresql import psycopg2

default_args = {
    'owner': 'Sergey',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='football_matches_dag',
    description='Fetching data of football matches and load to database and S3',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule='@daily',
    catchup=False,
    tags=['football', 'etl', 's3yandex' ]
)

def football_etl_matches():


    LEAGUES = ['pl', 'bl', 'ded', 'bsa', 'pd', 'fl1', 'ppl', 'sa']

    @task(pool="football_api_pool")
    def extract_matches(league: str) -> dict:
        try:

            week_ago = date.today() - timedelta(days=7)
            yesterday = date.today() - timedelta(days=1)
            params = {'dateFrom': str(week_ago), 'dateTo': str(yesterday)}

            api_key = Variable.get('api_key')

            endpoint_url = Variable.get(f'{league}_matches')

            headers = {"X-Auth-Token": api_key}
            response = requests.get(endpoint_url, headers=headers, params=params)
            response.raise_for_status()

            return {'league': league, 'data': response.json()}

        except Exception as e:
            raise Exception(f'API connection failed. Error: {e}')

    @task()
    def transform_matches(raw: dict) -> list[dict]:

        data = raw['data']

        finished_matches = [m for m in data['matches'] if m.get('status') == 'FINISHED']



        result = []
        for match in finished_matches:
            try:
                referee_id = match['referee'][0]['id']
                referee_name = match['referee'][0]['name']

            except(IndexError, KeyError):
                referee_id = None
                referee_name = None

            result.append({
                'match_id': match['id'],
                'competition_id': data['competition']['id'],
                'home_team_id': match['homeTeam']['id'],
                'home_team': match['homeTeam']['name'],
                'away_team_id': match['awayTeam']['id'],
                'away_team': match['awayTeam']['name'],
                'scores_home_team': match['score']['fullTime']['home'],
                'scores_away_team': match['score']['fullTime']['away'],
                'referee_id': referee_id,
                'referee_name': referee_name
            })

        return result

    @task()
    def load_raw_matches_to_s3(extracted: dict):

        acc_key = Variable.get('YC_ACCESS_KEY')
        sec_key = Variable.get('YC_SEC_KEY')
        bucket = Variable.get('YC_BUCKET')

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=acc_key,
            aws_secret_access_key=sec_key
        )

        league = extracted['league']
        raw_data = extracted['data']

        yesterday = date.today() - timedelta(days=1)
        key = f'raw/matches/{league}/{yesterday}.json'

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(raw_data, ensure_ascii=False).encode('utf-8')
        )

    @task()
    def load_matches_db(matches_data: list) -> None:

        connection = Variable.get("DB_CONNECTION")
        engine = create_engine(connection)

        all_matches = []
        for league_batch in matches_data:
            for match in league_batch:
                all_matches.append(match)

        data = pd.DataFrame(all_matches)
        data = data.drop_duplicates(subset='match_id', keep='first')

        records = data.to_dict(orient='records')

        upsert_query = text('''
                            INSERT INTO matches (match_id, competition_id, home_team_id, home_team,
                                                 away_team_id, away_team, scores_home_team,
                                                 scores_away_team, referee_id, referee_name)
                            VALUES (:match_id, :competition_id, :home_team_id, :home_team,
                                    :away_team_id, :away_team, :scores_home_team,
                                    :scores_away_team, :referee_id, :referee_name)
                            ON CONFLICT (match_id) DO UPDATE SET scores_home_team = EXCLUDED.scores_home_team,
                                                                 scores_away_team = EXCLUDED.scores_away_team,
                                                                 referee_id       = EXCLUDED.referee_id,
                                                                 referee_name     = EXCLUDED.referee_name
                            ''')

        with engine.begin() as conn:
            for record in records:
                conn.execute(upsert_query, record)

    @task()
    def sync_matches_to_clickhouse():

        pg_conn = psycopg2.connect(Variable.get("DB_CONNECTION"))
        cursor = pg_conn.cursor()
        cursor.execute("SELECT * FROM matches")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        ch_client = clickhouse_connect.get_client(
            host='clickhouse',
            port=8123,
            username='murasakir1n'
        )

        ch_client.insert('matches', rows,column_names=columns)


    raw = extract_matches.expand(league=LEAGUES)
    load_raw_matches_to_s3.expand(extracted=raw)

    matches_data = transform_matches.expand(raw=raw)

    load_matches_db(matches_data)

    sync_matches_to_clickhouse()

football_etl_matches()