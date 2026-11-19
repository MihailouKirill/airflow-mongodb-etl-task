"""
Loads the cleaned CSV file into main MongoDB collection ('reviews') and Triggers parallel computing of three
analytical data marts. Runs automatically when 'CLEANED_PATH' asset is updated
"""
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Asset, dag, task, task_group

from utils.config import CLEANED_PATH, TABLE_NAME
from utils.load import get_mongo_session, load_to_mongo
from utils.mongo_agg import avg_rating_by_day, less_than_5_symb, top_5_fr


@dag(
    dag_id = 'load_into_mongodb',
    start_date=datetime(2000, 11, 16),
    schedule = [Asset(str(CLEANED_PATH))],
    catchup=False,
    default_args = {
        "retries":2,
        "retry_delay": timedelta(seconds=30),
    },
    doc_md= __doc__ ,
)
def load_into_mongodb():


    @task
    def load_to_db():
        with get_mongo_session() as (client, db_name):
            load_to_mongo(
                client = client,
                file_path = CLEANED_PATH,
                db_name = db_name,
                table_name = TABLE_NAME )

        return TABLE_NAME

    @task_group(prefix_group_id=False)
    def mongo_agg_group(collection_name:Any):
        @task
        def top_5_frequently(col_name:Any):
            with get_mongo_session() as (client, db_name):
                top_5_fr(client = client,
                         db_name = db_name,
                         table_name = col_name,
                         target_table_name = "top_5_frequently_comments" )

        @task
        def content_less_than_5(col_name:Any):
            with get_mongo_session() as (client, db_name):
                less_than_5_symb(client = client,
                                 db_name = db_name,
                                 table_name = col_name,
                                 target_table_name = "content_less_than_5_symb" )
        @task
        def avg_rat_by_day(col_name:Any):
            with get_mongo_session() as (client, db_name):
                avg_rating_by_day(client = client,
                                  db_name = db_name,
                                  table_name = col_name,
                                  target_table_name = "avg_rating_by_day" )

        top_5_frequently(collection_name)
        content_less_than_5(collection_name)
        avg_rat_by_day(collection_name)



    loaded_collection = load_to_db()
    mongo_agg_group(loaded_collection)

load_into_mongodb()