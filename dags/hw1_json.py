from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from requests import get
from json import dumps

URL = 'https://raw.githubusercontent.com/LearnWebCode/json-example/refs/heads/master/pets-data.json'


@dag(
    start_date=datetime(2026, 2, 4),
    schedule="@daily",
    catchup=False,
)
def github_json_to_postgres():
    @task
    def extract():
        print(get(URL).text)
        return get(URL).json()

    @task
    def transform(data):
        rows = data['pets']

        for row in rows:
            if 'favFoods' in row:
                row['favFoods'] = dumps(row['favFoods'])

        return rows

    @task
    def load(rows):
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        cur = conn.cursor()

        for r in rows:
            cur.execute(
                """
                INSERT INTO pets (name, species, "favFoods", birthyear, photo) 
                VALUES (%s, %s, %s, %s, %s)
                """,
                (r.get('name'), r.get('species'), r.get('favFoods'), r.get('birthYear'), r.get('photo'))
            )

        conn.commit()

    load(transform(extract()))


dag = github_json_to_postgres()
