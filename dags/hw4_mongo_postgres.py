from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from psycopg2.extras import execute_values
from datetime import datetime
from json import dumps


@dag(
    start_date=datetime(2026, 2, 4),
    schedule="@daily",
    catchup=False,
)
def mongo_to_postgres_replication():

    @task
    def extract():
        mongo = MongoHook(mongo_conn_id="mongo").get_conn()
        db = mongo["users"]

        sessions_ts = Variable.get("sessions_last_ts", default_var="1970-01-01")
        events_ts = Variable.get("events_last_ts", default_var="1970-01-01")
        tickets_ts = Variable.get("tickets_last_ts", default_var="1970-01-01")
        recs_ts = Variable.get("recommendations_last_ts", default_var="1970-01-01")
        reviews_ts = Variable.get("reviews_last_ts", default_var="1970-01-01")

        sessions_pipeline = [
            {"$match": {"start_time": {"$gt": sessions_ts}}},
            {"$sort": {"start_time": -1}},
            {"$group": {
                "_id": "$session_id",
                "doc": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {
                "_id": {"$toString": "$_id"},
                "session_id": 1,
                "user_id": 1,
                "start_time": 1,
                "end_time": 1,
                "pages_visited": 1,
                "device": 1,
                "actions": 1
            }}
        ]
        sessions = list(db.UserSessions.aggregate(sessions_pipeline))

        events_pipeline = [
            {"$match": {"timestamp": {"$gt": events_ts}}},
            {"$sort": {"timestamp": -1}},
            {"$group": {"_id": "$event_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {
                "_id": {"$toString": "$_id"},
                "event_id": 1,
                "timestamp": 1,
                "event_type": 1,
                "details": 1
            }}
        ]
        events = list(db.EventLogs.aggregate(events_pipeline))

        tickets_pipeline = [
            {"$match": {"$or": [
                {"created_at": {"$gt": tickets_ts}},
                {"updated_at": {"$gt": tickets_ts}}
            ]}},
            {"$sort": {"updated_at": -1}},
            {"$group": {"_id": "$ticket_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {
                "_id": {"$toString": "$_id"},
                "ticket_id": 1,
                "user_id": 1,
                "status": 1,
                "issue_type": 1,
                "messages": 1,
                "created_at": 1,
                "updated_at": 1
            }}
        ]
        tickets = list(db.SupportTickets.aggregate(tickets_pipeline))

        recs_pipeline = [
            {"$match": {"last_updated": {"$gt": recs_ts}}},
            {"$sort": {"last_updated": -1}},
            {"$group": {"_id": "$user_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {
                "_id": {"$toString": "$_id"},
                "user_id": 1,
                "recommended_products": 1,
                "last_updated": 1
            }}
        ]
        recommendations = list(db.UserRecommendations.aggregate(recs_pipeline))

        reviews_pipeline = [
            {"$match": {"submitted_at": {"$gt": reviews_ts}}},
            {"$sort": {"submitted_at": -1}},
            {"$group": {"_id": "$review_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$project": {
                "_id": {"$toString": "$_id"},
                "review_id": 1,
                "user_id": 1,
                "product_id": 1,
                "review_text": 1,
                "rating": 1,
                "moderation_status": 1,
                "flags": 1,
                "submitted_at": 1
            }}
        ]
        reviews = list(db.ModerationQueue.aggregate(reviews_pipeline))

        return {
            "sessions": sessions,
            "events": events,
            "tickets": tickets,
            "recommendations": recommendations,
            "reviews": reviews
        }

    @task
    def transform(data):
        result = {"sessions": [
            (
                r["session_id"],
                r["user_id"],
                r.get("start_time"),
                r.get("end_time"),
                dumps(r.get("pages_visited")),
                dumps(r.get("device")),
                dumps(r.get("actions"))
            )
            for r in data["sessions"]
        ], "events": [
            (
                r["event_id"],
                r.get("timestamp"),
                r.get("event_type"),
                dumps(r.get("details"))
            )
            for r in data["events"]
        ], "tickets": [
            (
                r["ticket_id"],
                r.get("user_id"),
                r.get("status"),
                r.get("issue_type"),
                dumps(r.get("messages")),
                r.get("created_at"),
                r.get("updated_at")
            )
            for r in data["tickets"]
        ], "recommendations": [
            (
                r["user_id"],
                dumps(r.get("recommended_products")),
                r.get("last_updated")
            )
            for r in data["recommendations"]
        ], "reviews": [
            (
                r["review_id"],
                r.get("user_id"),
                r.get("product_id"),
                r.get("review_text"),
                r.get("rating"),
                r.get("moderation_status"),
                dumps(r.get("flags")),
                r.get("submitted_at")
            )
            for r in data["reviews"]
        ]}

        return result

    @task
    def load(data):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cur = conn.cursor()

        execute_values(cur, """
        INSERT INTO user_sessions
        VALUES %s
        ON CONFLICT (session_id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        pages_visited = EXCLUDED.pages_visited,
        device = EXCLUDED.device,
        actions = EXCLUDED.actions
        """, data["sessions"])

        execute_values(cur, """
        INSERT INTO event_logs
        VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
        timestamp = EXCLUDED.timestamp,
        event_type = EXCLUDED.event_type,
        details = EXCLUDED.details
        """, data["events"])

        execute_values(cur, """
        INSERT INTO support_tickets
        VALUES %s
        ON CONFLICT (ticket_id) DO UPDATE SET
        status = EXCLUDED.status,
        issue_type = EXCLUDED.issue_type,
        messages = EXCLUDED.messages,
        updated_at = EXCLUDED.updated_at
        """, data["tickets"])

        execute_values(cur, """
        INSERT INTO user_recommendations
        VALUES %s
        ON CONFLICT (user_id) DO UPDATE SET
        recommended_products = EXCLUDED.recommended_products,
        last_updated = EXCLUDED.last_updated
        """, data["recommendations"])

        execute_values(cur, """
        INSERT INTO moderation_queue
        VALUES %s
        ON CONFLICT (review_id) DO UPDATE SET
        review_text = EXCLUDED.review_text,
        rating = EXCLUDED.rating,
        moderation_status = EXCLUDED.moderation_status,
        flags = EXCLUDED.flags
        """, data["reviews"])

        conn.commit()

    load(transform(extract()))


dag = mongo_to_postgres_replication()
