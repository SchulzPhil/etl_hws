from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


@dag(
    start_date=datetime(2026, 3, 9),
    schedule="@daily",
    catchup=False,
)
def user_activity_refresh_dag():

    @task
    def refresh_vw():
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("TRUNCATE TABLE user_activity_vw;")

        cur.execute("""        
INSERT INTO user_activity_vw (user_id, session_count, total_seconds, popular_pages, popular_actions)
SELECT
    us.user_id,
    COUNT(*) AS session_count,
    SUM(EXTRACT(EPOCH FROM (us.end_time - us.start_time))) AS total_seconds,
    ARRAY_AGG(DISTINCT p.page) AS popular_pages,
    ARRAY_AGG(DISTINCT a.action) AS popular_actions
FROM user_sessions us
LEFT JOIN LATERAL jsonb_array_elements_text(us.pages_visited) AS p(page) ON TRUE
LEFT JOIN LATERAL jsonb_array_elements_text(us.actions) AS a(action) ON TRUE
GROUP BY us.user_id;
        """)
        conn.commit()

    refresh_vw()


dag = user_activity_refresh_dag()
