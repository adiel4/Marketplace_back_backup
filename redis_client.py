import redis
import json


class CustomRedis:
    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.Redis(host=host, port=port, db=db)

    def get_cached_value(self, name: str):
        value_str = self.redis_client.get(name)
        value_arr = json.loads(value_str)
        return value_arr

    def set_cached_value(self, value_arr, name: str):
        value_str = json.dumps(value_arr)
        self.redis_client.set(name, value_str)
