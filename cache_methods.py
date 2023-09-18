import datetime
import json
from datetime import timedelta

import redis
import decimal


def get_cached_value(redis_client: redis.Redis, name: str):
    value_str = redis_client.get(name)
    value_arr = json.loads(value_str)
    return value_arr


def set_cached_value(redis_client: redis.Redis, value_arr, name: str):
    value_str = json.dumps(value_arr, default=decimal_encoder)
    redis_client.set(name, value_str)
    return value_arr


def set_cached_value_by_days(redis_client: redis.Redis, value_arr, name: str, expire_days: int):
    value_str = json.dumps(value_arr, default=decimal_encoder)
    redis_client.set(name, value_str, ex=timedelta(days=expire_days))
    return value_arr


def set_cached_value_by_minutes(redis_client: redis.Redis, value_arr, name: str, expire_minutes: int):
    value_str = json.dumps(value_arr, default=decimal_encoder)
    redis_client.set(name, value_str, ex=timedelta(minutes=expire_minutes))
    return value_arr


def decimal_encoder(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)  # Convert Decimal to string representation
    elif isinstance(obj, datetime.date):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable.{obj}")
