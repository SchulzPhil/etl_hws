from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kagglehub import dataset_download
from psycopg2.extras import execute_values
from pandas import DataFrame, read_csv, to_datetime


@dag(
    start_date=datetime(2026, 2, 4),
    schedule="@daily",
    catchup=False,
)
def kaggel_to_postgres():

    @task
    def extract():
        path = dataset_download("atulanandjha/temperature-readings-iot-devices")
        return read_csv(f'{path}/IOT-temp.csv')

    @task
    def transform(df: DataFrame):
        df = df[df["out/in"] == "In"].copy()
        df["noted_date"] = to_datetime(df["noted_date"], errors="coerce").dt.date
        df.dropna(inplace=True)

        p5, p95 = df["temp"].quantile([0.05, 0.95])
        df = df[df["temp"].between(p5, p95)]

        coldest_5 = (
            df.loc[df.groupby("noted_date")["temp"].idxmin()]
            .sort_values("temp", ascending=True)
            .head(5)
        )

        hottest_5 = (
            df.loc[df.groupby("noted_date")["temp"].idxmax()]
            .sort_values("temp", ascending=False)
            .head(5)
        )

        return (
            [list(row.values()) for row in coldest_5.to_dict('records')],
            [list(row.values()) for row in hottest_5.to_dict('records')],
            [list(row.values()) for row in df.to_dict('records')]
        )

    @task
    def load(data):
        min_temp_days, max_temp_days, calendar = data
        hook = PostgresHook(postgres_conn_id='postgres')
        conn = hook.get_conn()
        cur = conn.cursor()

        execute_values(
            cur,
            'INSERT INTO iot_temperature_thresholds '
            'VALUES %s',
            (min_temp_days + max_temp_days)
        )

        execute_values(
            cur,
            'INSERT INTO iot_temperature_calendar '
            'VALUES %s',
            calendar
        )

        conn.commit()

    load(transform(extract()))


dag = kaggel_to_postgres()
