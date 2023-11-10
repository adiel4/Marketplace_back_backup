import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
# Database config
host = os.getenv('HOST')
port = int(os.getenv('PORT'))
charset = os.getenv('DB_CHARSET')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
role = os.getenv('ROLE')
# FSDatabase config
fs_host = os.getenv('FS_HOST')
fs_port = int(os.getenv('FS_PORT'))
fs_charset = os.getenv('FS_DB_CHARSET')
fs_username = os.getenv('FS_DB_USER')
fs_password = os.getenv('FS_DB_PASSWORD')
fs_db_name = os.getenv('FS_DB_NAME')
fs_role = os.getenv('FS_ROLE')
# FastApi config
app_host = os.getenv('APP_HOST')
app_port = int(os.getenv('APP_PORT'))
# Redis config
redis_url = os.getenv('REDIS_URL')
redis_port = int(os.getenv('REDIS_PORT'))
# Minio config
minio_endpoint = os.getenv('MINIO_ENDPOINT')
minio_user = os.getenv('MINIO_USER')
minio_password = os.getenv('MINIO_PASSWORD')
# yandex
yandex_api_key = os.getenv('YANDEX_API_KEY')
# Goggle
google_api_key = os.getenv('GOOGLE_API_KEY')
# Mongo
mongo_host = os.getenv('MONGO_HOST')
mongo_port = os.getenv('MONGO_PORT')

def_search_sort_types = '''{
"date": 0,
"price": 0
}'''
