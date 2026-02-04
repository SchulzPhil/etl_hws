from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import xmltodict

URL = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"
PG_CONN_ID = "postgres"


@dag(
    dag_id="xml_nutrition_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["xml", "nutrition"],
)
def xml_nutrition_pipeline():

    @task
    def fetch() -> str:
        r = requests.get(URL, timeout=30)
        return r.text

    @task
    def parse(xml_text: str) -> dict:
        data = xmltodict.parse(xml_text)["nutrition"]

        daily = []
        for k, v in data["daily-values"].items():
            daily.append({
                "nutrient": k.replace("-", "_"),
                "value": float(v["#text"]),
                "unit": v["@units"],
            })

        foods = []
        for f in data["food"]:
            foods.append({
                "name": f["name"],
                "manufacturer": f.get("mfr"),
                "serving_value": float(f["serving"]["#text"]),
                "serving_unit": f["serving"].get("@units"),
                "calories_total": int(f["calories"]["@total"]),
                "calories_fat": int(f["calories"]["@fat"]),
                "nutrients": {
                    "total_fat": float(f["total-fat"]),
                    "saturated_fat": float(f["saturated-fat"]),
                    "cholesterol": float(f["cholesterol"]),
                    "sodium": float(f["sodium"]),
                    "carb": float(f["carb"]),
                    "fiber": float(f["fiber"]),
                    "protein": float(f["protein"]),
                },
                "vitamins": f["vitamins"],
                "minerals": f["minerals"],
            })

        return {"daily": daily, "foods": foods}

    @task
    def load_daily_values(payload: dict):
        hook = PostgresHook(PG_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        for d in payload["daily"]:
            cur.execute(
                """
                INSERT INTO daily_values (nutrient, value, unit)
                VALUES (%s, %s, %s)
                ON CONFLICT (nutrient)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    unit = EXCLUDED.unit
                """,
                (d["nutrient"], d["value"], d["unit"]),
            )

        conn.commit()
        cur.close()
        conn.close()

    @task
    def load_foods(payload: dict):
        hook = PostgresHook(PG_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        for f in payload["foods"]:
            cur.execute(
                """
                INSERT INTO foods
                (name, manufacturer, serving_value, serving_unit,
                 calories_total, calories_fat)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    f["name"],
                    f["manufacturer"],
                    f["serving_value"],
                    f["serving_unit"],
                    f["calories_total"],
                    f["calories_fat"],
                ),
            )
            food_id = cur.fetchone()[0]

            for n, v in f["nutrients"].items():
                cur.execute(
                    """
                    INSERT INTO food_nutrients (food_id, nutrient, value)
                    VALUES (%s, %s, %s)
                    """,
                    (food_id, n, v),
                )

            for k, v in f["vitamins"].items():
                cur.execute(
                    """
                    INSERT INTO food_micros (food_id, type, name, value)
                    VALUES (%s, 'vitamin', %s, %s)
                    """,
                    (food_id, k, float(v)),
                )

            for k, v in f["minerals"].items():
                cur.execute(
                    """
                    INSERT INTO food_micros (food_id, type, name, value)
                    VALUES (%s, 'mineral', %s, %s)
                    """,
                    (food_id, k, float(v)),
                )

        conn.commit()
        cur.close()
        conn.close()

    xml = fetch()
    parsed = parse(xml)
    load_daily_values(parsed)
    load_foods(parsed)


dag = xml_nutrition_pipeline()
