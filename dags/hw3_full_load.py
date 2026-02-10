from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kagglehub import dataset_download
from psycopg2.extras import execute_values
from pandas import read_csv, to_datetime


@dag(
    start_date=datetime(2026, 2, 4),
    schedule=None,
    catchup=False
)
def iot_temperature_full_load():

    @task
    def extract():
        path = dataset_download("atulanandjha/temperature-readings-iot-devices")
        return read_csv(f"{path}/IOT-temp.csv")

    @task
    def transform(df):
        df = df[df["out/in"] == "In"].copy()
        df["noted_date"] = to_datetime(df["noted_date"], errors="coerce").dt.date
        df.dropna(subset=["noted_date", "temp"], inplace=True)

        p5, p95 = df["temp"].quantile([0.05, 0.95])
        df = df[df["temp"].between(p5, p95)]

        return list(
            df[["id", "noted_date", "temp", "room_id/id"]]
            .itertuples(index=False, name=None)
        )

    @task
    def load(rows):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE iot_temperature_calendar")

        execute_values(
            cur,
            """
            INSERT INTO iot_temperature_calendar
            (id, noted_date, temp, room_id)
            VALUES %s
            """,
            rows,
        )

        conn.commit()

    load(transform(extract()))


dag = iot_temperature_full_load()
