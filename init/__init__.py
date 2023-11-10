import redis
import config as cfg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import minio_client as mn_cli
from pymongo import MongoClient


app = FastAPI(
    title='Marketplace'
)

app_exception = HTTPException

origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://192.168.200.182",
    "https://test-marketplace.mbulak.kg",
    "https://test-marketplace.mbulak.kg:8000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

minio_client = mn_cli.CustomMinio(secure=True)
redis_client = redis.Redis(host=cfg.redis_url, port=cfg.redis_port, db=0)
client_mongo = MongoClient(f"mongodb://{cfg.mongo_host}:{cfg.mongo_port}/")["error_logs"]
