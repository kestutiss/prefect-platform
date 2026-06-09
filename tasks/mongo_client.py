from contextlib import contextmanager

from pymongo import MongoClient


@contextmanager
def get_mongo_client(uri: str):
    """Context-managed MongoClient — always closes the connection on exit."""
    client = MongoClient(uri)
    try:
        yield client
    finally:
        client.close()
