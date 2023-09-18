import redis
import config as cfg

redis_client = redis.Redis(host=cfg.redis_url, port=cfg.redis_port, db=0)

print(redis_client.keys())