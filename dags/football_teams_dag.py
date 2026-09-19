from datetime import datetime, timedelta
import json
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
import boto3
from sqlalchemy import create_engine
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Настройка для всех задач в Dag
default_args = {
    'owner': 'Sergey',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='football_teams_dag',
    description='Fetching data of football teams and load to database and S3',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule='@hourly',
    catchup=False,
    tags=['crypto', 'etl', 'yandex', 'gecko']
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


    raw = extract_teams.expand(league=LEAGUES)
    load_raw_teams_s3.expand(extracted=raw)

football_etl_teams()