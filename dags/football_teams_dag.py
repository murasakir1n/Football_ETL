from datetime import datetime, timedelta
import json
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
import boto3
from sqlalchemy import create_engine, text


default_args = {
    'owner': 'Sergey',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='football_teams_dag',
    description='Fetching data of football teams, players and load to database and S3',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule='@daily',
    catchup=False,
    tags=['football', 'etl', 's3yandex' ]
)
def football_etl_teams():

    LEAGUES = ['pl', 'bl', 'ded', 'bsa', 'pd', 'fl1', 'ppl', 'sa']

    @task(pool="football_api_pool")
    def extract_teams(league: str) -> dict:

        try:
            api_key = Variable.get("api_key")

            endpoint_url = Variable.get(f"{league}_teams")

            headers = {"X-Auth-Token": api_key}
            response = requests.get(endpoint_url, headers=headers)
            response.raise_for_status()


            return {"league": league, "data": response.json()}

        except Exception as e:
            raise Exception(f'API connection failed. Error: {e}')

    @task()
    def transform_teams(raw: dict) -> list[dict]:

        data = raw['data']

        return[
            {
                "team_id": team["id"],
                "name": team["name"],
                "abbreviation": team.get("tla"),
                "stadium": team.get("venue"),
                "address": team.get("address"),
                "competition_id": data["competition"]["id"],
            }
            for team in data["teams"]
        ]

    @task()
    def transform_players(raw: dict) -> list[dict]:

        data = raw['data']

        return [
                {
                    'player_id': player['id'],
                    'name': player['name'],
                    "team_id": team["id"],
                    'position': player.get('position'),
                    'date_of_birth': player.get('dateOfBirth'),
                    'nationality': player.get('nationality')
                }

            for team in data['teams']
            for player in team.get('squad', [])
        ]


    @task()
    def transform_competitions(raw: dict) -> list[dict]:

        competition = raw['data']['competition']

        return [{
            'competition_id': competition['id'],
            'competition_name': competition['name']
        }]



    @task()
    def load_raw_teams_s3(extracted: dict):

        acc_key = Variable.get('YC_ACCESS_KEY')
        sec_key = Variable.get('YC_SEC_KEY')
        bucket = Variable.get('YC_BUCKET')

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url = 'https://storage.yandexcloud.net',
            aws_access_key_id= acc_key,
            aws_secret_access_key= sec_key
        )

        league = extracted['league']
        raw_data = extracted['data']

        key = f'raw/teams/{league}.json'

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(raw_data, ensure_ascii=False).encode('utf-8')
        )

    @task()
    def clear_db() -> None:
        connection = Variable.get('DB_CONNECTION')
        engine = create_engine(connection)

        with engine.connect() as conn:
            conn.execute(text('TRUNCATE players, teams, competitions RESTART IDENTITY CASCADE;'))
            conn.commit()


    @task()
    def load_competition_db(competitions_data: list) -> None:

        connection = Variable.get('DB_CONNECTION')
        engine = create_engine(connection)

        all_competitions = []
        for league_batch in competitions_data:
            for competition in league_batch:
                all_competitions.append(competition)

        data = pd.DataFrame(all_competitions)

        data.to_sql(
            name='competitions',
            con=engine,
            if_exists='append',
            index=False
        )


    @task()
    def load_teams_db(teams_data: list) -> None:

        connection = Variable.get('DB_CONNECTION')
        engine = create_engine(connection)

        all_teams = []
        for league_batch in teams_data:
            for team in league_batch:
                all_teams.append(team)

        data = pd.DataFrame(all_teams)

        data.to_sql(
            name = 'teams',
            con=engine,
            if_exists='append',
            index=False
        )



    @task()
    def load_players_db(players_data: list) -> None:

        connection = Variable.get('DB_CONNECTION')
        engine = create_engine(connection)

        all_players = []
        for league_batch in players_data:
            for player in league_batch:
                all_players.append(player)

        data = pd.DataFrame(all_players)
        data = data.drop_duplicates(subset='player_id', keep='first')

        data.to_sql(
            name='players',
            con=engine,
            if_exists='append',
            index=False
        )

    raw = extract_teams.expand(league=LEAGUES)
    load_raw_teams_s3.expand(extracted=raw)

    teams_data = transform_teams.expand(raw=raw)
    players_data = transform_players.expand(raw=raw)
    competitions_data = transform_competitions.expand(raw=raw)


    clear_db()>>load_competition_db(competitions_data)>>load_teams_db(teams_data)>>load_players_db(players_data)

football_etl_teams()