"""
Gets MongoDB connection and loads file into MongoDB
"""
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from airflow.sdk import BaseHook
from pymongo import MongoClient


def mongo_connect(
            host:str ,
            port:int  ,
            user:str ,
            password:str ,
            extra: dict[str,Any] | None = None,
    )-> MongoClient:
    """
    Get MongoDB client

    :param host: db host
    :param port: db port
    :param user: db user
    :param password: db password
    :param extra: extra params
    :return: client: MongoDB client
    """

    client = MongoClient(
        host = host,
        port = port,
        username = user,
        password = password,
        **(extra or {}),
        )


    client.admin.command('ping')

    return client

@contextmanager
def get_mongo_session(conn_id :str = "mongodb") -> Generator[tuple[MongoClient, str]]:
    """
    Context manager to connect to MongoDB. Gets connection params with BaseHook

    :param conn_id: connection id for BaseHook
    :yield: client: MongoDB client
            conn.schema: MongoDB db name
    """
    conn =BaseHook.get_connection(conn_id = conn_id)
    if (
        not conn.host
        or conn.port is None
        or not conn.login
        or not conn.password
        or not conn.schema
    ):
        raise ValueError(f"Connection '{conn_id}' is missing required fields")


    with mongo_connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            extra=conn.extra_dejson,
    ) as client:
        yield client, conn.schema





def load_to_mongo(client : MongoClient,
                  file_path:str | Path,
                  db_name:str,
                  table_name:str) -> None:
    """
    Loads data from csv file to MongoDB and before loading cleans previous values to avoid duplicates

    :param client: MongoDB client
    :param file_path: Path to file which will be loaded into db
    :param db_name: MongoDB db name
    :param table_name: MongoDB table name
    """
    import pandas as pd

    file_path = Path(file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f'File not found at {file_path}')

    db = client[db_name]
    collection = db[table_name]

    df = pd.read_csv(file_path)
    records = df.to_dict('records')
    if records:
        collection.delete_many({})
        collection.insert_many(records)

