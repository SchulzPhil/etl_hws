from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kagglehub import dataset_download
from pandas import read_csv, to_datetime
from psycopg2.extras import execute_values


@dag(
    dag_id="iot_incremental_load",
    start_date=datetime(2026, 2, 10),
    schedule="@daily",
    catchup=False
)
def iot_incremental_load():

    @task
    def extract():
        path = dataset_download("atulanandjha/temperature-readings-iot-devices")
        return read_csv(f"{path}/IOT-temp.csv")

    @task
    def transform(df, execution_date: str):
        n_days = int(Variable.get("iot_incremental_days", default_var=3))

        df = df[df["out/in"] == "In"].copy()

        df["noted_date"] = to_datetime(df["noted_date"], errors="coerce").dt.date
        df.dropna(inplace=True)

        p5, p95 = df["temp"].quantile([0.05, 0.95])
        df = df[df["temp"].between(p5, p95)]

        exec_date = to_datetime(execution_date).date()
        start_date = exec_date - timedelta(days=n_days)
        df = df[df["noted_date"] >= start_date]

        rows = list(
            df[
                ["id", "noted_date", "temp", "room_id/id"]
            ].itertuples(index=False, name=None)
        )

        return rows, start_date

    @task
    def load(data):
        rows, start_date = data

        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            DELETE FROM iot_temperature_calendar
            WHERE noted_date >= %s
            """,
            (start_date,),
        )

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

    load(transform(extract(), "{{ ds }}"))


dag = iot_incremental_load()
