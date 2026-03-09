from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    start_date=datetime(2026, 3, 9),
    schedule="@daily",
    catchup=False,
)
def support_stats_refresh_dag():

    @task
    def refresh_vw():
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE support_stats_vw;")

        cur.execute("""
        INSERT INTO support_stats_vw (status, issue_type, ticket_count, avg_resolution_seconds, max_resolution_seconds)
        SELECT
            status,
            issue_type,
            COUNT(*) AS ticket_count,
            AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) AS avg_resolution_seconds,
            MAX(EXTRACT(EPOCH FROM (updated_at - created_at))) AS max_resolution_seconds
        FROM support_tickets
        GROUP BY status, issue_type;
        """)
        conn.commit()

    refresh_vw()


dag = support_stats_refresh_dag()
