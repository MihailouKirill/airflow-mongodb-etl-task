"""
Mongo aggregations module
"""
from pymongo import MongoClient


def top_5_fr(client: MongoClient,
             db_name:str,
             table_name:str,
             target_table_name: str = "top_5_comments") -> None:
    """
    Computes and saves Top 5 frequently occurring comments from MongoDB

    :param client:MongoClient object
    :param db_name: database name
    :param table_name: table name
    :param target_table_name: final target table name
    """

    db = client[db_name]
    collection = db[table_name]

    pipeline = [
        {"$match": {
            "content": {
                "$type": "string",
                "$nin": ["-", "", "nan", "NaN", "None", "null"],
            }
        }},
        {"$group": {"_id": "$content", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$limit": 5},
        {"$out":target_table_name}
    ]
    collection.aggregate(pipeline).to_list()

def less_than_5_symb(client: MongoClient,
                        db_name:str,
                        table_name:str,
                        target_table_name: str = "content_less_than_5") -> None:
    """
    Computes and saves All entries where the 'content' field is less than 5 characters long from MongoDB

    :param client:MongoClient object
    :param db_name: database name
    :param table_name: table name
    :param target_table_name: final target table name
    """

    db = client[db_name]
    collection = db[table_name]
    pipeline = [
        {"$match": {
            "content": {
                "$type": "string",
                "$nin": ["-", "", "nan", "NaN", "None", "null"]},
            }
        },
        {"$match":{
            "$expr": {"$lt": [{"$strLenCP": "$content"}, 5]}
        }
        },
        {"$out":target_table_name}
    ]
    collection.aggregate(pipeline).to_list()

def avg_rating_by_day(client: MongoClient,
                      db_name:str,
                      table_name:str,
                      target_table_name: str = "avg_rating_by_day") -> None:
    """
    Computes and saves Average rating for each day from MongoDB

    :param client:MongoClient object
    :param db_name: database name
    :param table_name: table name
    :param target_table_name: final target table name
    """

    db = client[db_name]
    collection = db[table_name]
    pipeline = [
        {"$match": {
            "score":{"$type": "number", "$ne":float("NaN")},
            "at" : {"$type": "string", "$nin": ["-", "", "nan", "NaN", "None", "null"]},
        }},
        {"$group": {"_id": {"$dateTrunc":{"date":{"$toDate":"$at"},
                                         "unit":"day",
                                         }
                            },
                    "avg_score":{"$avg":"$score"},
                    }
         },
        {"$sort": {"_id":1}},
        {"$out":target_table_name}
    ]
    collection.aggregate(pipeline).to_list()