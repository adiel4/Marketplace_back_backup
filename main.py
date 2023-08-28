import base64
import json
import random
from io import BytesIO
from uuid import uuid4
from pyzbar.pyzbar import decode
import redis
import uvicorn
from PIL import Image
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.exceptions import ConnectionError
import isolated as iso
import cache_methods as ch_meth
import config as cfg
import database as db
import database_methods as db_meth
import fs_database_methods as fs_db_meth
import minio_client as mn_cli
import models
import image_methods
import qr

app = FastAPI(
    title='Marketplace'
)

origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://192.168.200.182"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

redis_client = redis.Redis(host=cfg.redis_url, port=cfg.redis_port, db=0)

minio_client = mn_cli.CustomMinio(secure=True)


@app.get("/")
async def get_main():
    return {"status": 0}


@app.post('/items.{item_id}')
async def put_item(process_name: str, item: models.Item):
    if process_name in ['category', 'subcategory', 'brand']:
        result = db_meth.insert_item(process_name, item.json())
        if result['status'] == 0 or result['err_msg'] == '':
            redis_client.delete('subcategories')
            redis_client.delete('brands')
            redis_client.delete('categories')
            return result
        else:
            return {'status': -1, 'err_msg': result['err_msg']}
    else:
        return {'status': -1, 'err_msg': 'Process name is invalid'}
    pass


@app.get('/subcategories')
async def get_subcategories(cat_id: int):
    if redis_client.exists(f'category:{str(cat_id)}:subcategories'):
        return ch_meth.get_cached_value(redis_client, f'category:{str(cat_id)}:subcategories')
    value_arr = db_meth.get_subcategories(cat_id)
    if value_arr:
        return ch_meth.set_cached_value(redis_client, value_arr, f'category:{str(cat_id)}:subcategories')


@app.get("/categories")
async def get_categories():
    if redis_client.exists('categories'):
        return ch_meth.get_cached_value(redis_client, 'categories')
    value_arr = db_meth.get_categories()
    if value_arr:
        return ch_meth.set_cached_value(redis_client, value_arr, 'categories')


@app.get("/brands")
async def get_brands():
    if redis_client.exists('brands'):
        return ch_meth.get_cached_value(redis_client, 'brands')
    value_arr = db_meth.get_brands()
    if value_arr:
        return ch_meth.set_cached_value(redis_client, value_arr, 'brands')


@app.post('/clear_cache')
async def clean_cache():
    try:
        redis_client.flushall()
    except Exception as err:
        return {"err_msg": format(err)}

    await get_brands()
    value = await get_categories()
    categs = value['categories']
    for categ in categs:
        await get_subcategories(categ.get('id'))
    return {"err_msg": "Кэш очищен"}


@app.get("/get_good")
async def get_good(g_id: int):
    if redis_client.exists(f'good:{g_id}'):
        return ch_meth.get_cached_value(redis_client, f'good:{g_id}')
    value_arr = db_meth.get_good(g_id)
    if value_arr:
        return ch_meth.set_cached_value(redis_client, value_arr, f'good:{g_id}')


@app.get("/get_goods")
async def get_goods(quantity: int, params: dict = None):
    return db_meth.get_goods(quantity, params)


@app.post('/add_good')
async def add_good(good: models.Good):
    try:
        result = db_meth.add_good(good)
        g_id = result[0]['g_id']
        image_list = good.images
        for item in image_list:
            item['item_id'] = g_id
        res = image_methods.add_images(basket='goods', images=image_list)
        if res['status'] != 0:
            return {'status': -1, 'err_msg': res['err_msg']}
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}
    else:
        redis_client.delete('goods')
        iso.get_goods_cache()
        return result


@app.post('/del_good')
async def del_good(g_id: models.PrimaryKey):
    value = db_meth.del_good(g_id)
    redis_client.delete('goods')
    iso.get_goods_cache()
    return value


@app.post('/de_active_good')
async def de_active_good(action: models.SellerGoodAction):
    value = db_meth.de_active_good(action)
    redis_client.delete('goods')
    iso.get_goods_cache()
    return value


@app.get("/get_good_images")
async def get_good_images(g_id: int, only_main: int):
    return minio_client.get_goods_images(g_id, only_main, 'goods')


@app.get("/get_market_images")
async def get_market_images(m_id: int, only_main: int):
    return minio_client.get_goods_images(m_id, only_main, 'markets')


@app.post("/edit_good_images")
async def edit_good_images(editimage: models.EditImage):
    return image_methods.edit_images(editImage=editimage)


@app.post('/add_good_image')
async def add_good_images(good_images: list):
    return image_methods.add_images(basket='goods', images=good_images)


@app.get("/get_categories_levels")
async def get_categories_levels():
    return db_meth.get_categories_levels()


@app.post('/user_token')
async def get_user_token(user: models.User):
    token = str(uuid4())
    c_id = user.c_id
    user_type = user.user_type
    if redis_client.exists(f'{c_id}_{user_type}'):
        return ch_meth.get_cached_value(redis_client, f'{c_id}_{user_type}')
    values = db_meth.get_token(c_id, user_type, token)
    if values:
        values['out_expire_datetime'] = values['out_expire_datetime'].strftime("%Y-%m-%d %H:%M:%S.%f")
        return ch_meth.set_cached_value_by_days(redis_client, values, f'{c_id}_{user_type}', expire_days=7)


@app.get('/user_info')
async def user_info(c_id: int):
    return fs_db_meth.get_client_info(c_id)


@app.post('/add_good_to_basket')
async def add_good_to_basket(c_id: int, good: models.Basket):
    values = db_meth.add_good_to_basket(good)
    basket_values = db_meth.get_basket_content(c_id)
    if basket_values:
        ch_meth.set_cached_value(redis_client, basket_values, f'basket:{c_id}')
    return values


@app.get('/get_basket_content')
async def get_basket_content(c_id: int):
    if redis_client.exists(f'basket:{c_id}'):
        return ch_meth.get_cached_value(redis_client, f'basket:{c_id}')
    value = db_meth.get_basket_content(c_id)
    if value:
        return ch_meth.set_cached_value(redis_client, value, f'basket:{c_id}')


@app.get("/brand")
async def get_brand(b_id: int):
    return db_meth.get_brand(b_id)


@app.get("/get_models")
async def get_models(cat_id: int = 0, b_id: int = 0):
    return db_meth.get_models(cat_id, b_id)


@app.post("/insert_new_item")
async def insert_new_item(body: models.NewItem):
    value = db_meth.insert_new_item(body)
    redis_client.delete('goods')
    iso.get_goods_cache()
    return value


@app.get("/get_seller_add_apps")
async def get_seller_add_apps(c_id: int):
    return db_meth.get_seller_add_apps(c_id)


@app.get("/get_seller_sell_apps")
async def get_seller_sell_apps(c_id: int):
    return db_meth.get_seller_sell_apps(c_id)


@app.post("/del_basket_good")
async def del_basket_good(delGood: models.DelGoodBasket):
    values = db_meth.delete_good_from_basket(delGood.c_id, delGood.g_id)
    basket_values = db_meth.get_basket_content(delGood.c_id)
    ch_meth.set_cached_value(redis_client, basket_values, f'basket:{delGood.c_id}')
    return values


@app.post("/clear_basket")
async def clear_basket(delGood: models.DelGoodBasket):
    values = db_meth.clear_basket(delGood.c_id)
    if values.get('status') == 0:
        ch_meth.set_cached_value(redis_client, None, f'basket:{delGood.c_id}')
    return values


@app.get("/get_seller_markets")
async def get_seller_markets(c_id: int):
    return db_meth.get_seller_markets(c_id)


@app.get("/get_market_store")
async def get_market_store(m_id: int, is_active: bool):
    return db_meth.get_market_store(m_id, is_active)


@app.get("/get_market_store_on_mod")
async def get_market_store_on_mod(m_id: int):
    return db_meth.get_market_store_on_mod(m_id)


@app.get("/parent_category")
async def parent_category(cat_id: int):
    return db_meth.parent_category(cat_id)


@app.get("/get_app_result")
async def get_app_result(al_id: int):
    return db_meth.get_app_result(al_id)


@app.post("/red_good")
async def red_good(good: models.Good):
    value = db_meth.red_good(good)
    redis_client.delete('goods')
    iso.get_goods_cache()
    return value


@app.get("/get_notifications")
async def get_notifications(c_id: int):
    return db_meth.get_notifications(c_id)


@app.get("/get_unread_notifications")
async def get_unread_notifications(c_id: int):
    return db_meth.get_unread_notifications(c_id)


@app.post("/read_notification")
async def read_notification(notification: models.Notifications):
    return db_meth.read_notification(notification)


@app.get("/get_regions")
async def get_regions(reg_id: int = 1000000):
    return fs_db_meth.get_regions(reg_id)


@app.post("/create_miniapp")
async def create_miniapp(c_id: int):
    value = db_meth.create_miniapp(c_id)
    return value


@app.get("/create_qr")
async def create_qr(c_id: int):
    if redis_client.exists(f'c_id:{c_id}:qr'):
        return {key.encode('utf-8'): value for key, value in
                ch_meth.get_cached_value(redis_client, f'c_id:{c_id}:qr').items()}
    data = None
    try:
        data = db_meth.get_waitlist(c_id)
    except Exception as err:
        print(err)
    if data is None:
        return {'status': -1, 'err_msg': 'No active waitlist'}
    json_value = str(data)
    value = qr.save_qr(data=json_value, c_id=c_id)
    qr_code = random.randint(10000000, 99999999)
    value['qr_code'] = qr_code
    if value:
        ch_meth.set_cached_value_by_days(redis_client, value, f'qr_code:{qr_code}:qr', expire_days=1)
        return ch_meth.set_cached_value_by_days(redis_client, value, f'c_id:{c_id}:qr', expire_days=1)


@app.get("/scan_qr")
async def scan_qr(operation_type: str, data: str, m_id: int):
    encrypted_data = None
    cli_c = None
    if operation_type == 'qr_code':
        if redis_client.exists(f'qr_code:{int(data)}:qr'):
            decrypt_data = ch_meth.get_cached_value(redis_client, f'qr_code:{int(data)}:qr')
            data = decrypt_data['qr_base64']
            base64_qr_code = data
            base64_data = base64_qr_code.split(',')[1]
            image_data = base64.b64decode(base64_data)
            image_stream = BytesIO(image_data)
            image = Image.open(image_stream)
            decoded_qr_code = decode(image)
            if decoded_qr_code:
                data = decoded_qr_code[0].data.decode('utf-8')
            else:
                return {'status': -1, 'err_msg': "No QR code found in the image"}
            cli_c = int(data[:13])
            encrypted_data = data[13:]
        else:
            return {'status': -1, 'err_msg': "Unknown QR"}
    elif operation_type == "qr":
        try:
            cli_c = int(data[:13])
            encrypted_data = data[13:]
        except Exception as err:
            print(err)
            return {'status': -1, 'err_msg': "Unknown QR"}
    else:
        return {'status': -1, 'err_msg': "Unknown operation type"}
    value = None
    if redis_client.exists(f'c_id:{cli_c}:qr'):
        decrypt_data = ch_meth.get_cached_value(redis_client, f'c_id:{cli_c}:qr')
    else:
        return {'status': -1, 'err_msg': "Unknown QR"}
    decrypt_key = decrypt_data['key'].encode('utf-8')
    if decrypt_key is None or decrypt_data is None:
        return {'status': -1, 'err_msg': "Unknown QR"}
    try:
        value = qr.decrypt_data(encrypted_data, decrypt_key)
    except Exception as err:
        print(err)
    datalist = json.loads(value.replace("'", "\""))
    g_id_values = [int(item['g_id']) for item in datalist]
    g_id_string = ','.join([str(item) for item in g_id_values])
    market_goods = db_meth.market_good_from_wl(m_id, g_id_string)
    if market_goods is None:
        return {'status': -1, 'err_msg': "No goods for client"}
    else:
        for good in market_goods:
            good['g_name'] = datalist[g_id_values.index(good['g_id'])]['g_name']
            good['g_qty'] = datalist[g_id_values.index(good['g_id'])]['g_qty']
        return market_goods


@app.get("/get_market_info")
def get_market_info(m_id: int):
    if redis_client.exists(f'market_info:{m_id}'):
        return ch_meth.get_cached_value(redis_client, f'market_info:{m_id}')
    value_arr = db_meth.get_market_info(m_id)
    if value_arr:
        return ch_meth.set_cached_value(redis_client, value_arr, f'market_info:{m_id}')


@app.get("/get_market_contacts")
def get_market_contacts(m_id: int):
    if redis_client.exists(f'market_contacts:{m_id}'):
        return ch_meth.get_cached_value(redis_client, f'market_contacts:{m_id}')
    value_arr = db_meth.get_market_contacts(m_id)
    if value_arr:
        return ch_meth.set_cached_value(redis_client, value_arr, f'market_contacts:{m_id}')


@app.post("/redact_market_info")
def redact_market_info(mi: models.MarketInfo):
    return db_meth.redact_market_info(mi)


@app.post("/redact_market_contacts")
async def redact_market_contacts(mc: models.MarketContacts):
    return db_meth.redact_market_contacts(mc)


@app.get("/get_region_name")
async def get_region_name(reg_id: int):
    return fs_db_meth.get_region_name(reg_id)


@app.post("/create_waitlist")
async def create_waitlist(waitlist: models.Waitlist):
    res = db_meth.create_waitlist(waitlist)
    basket_values = db_meth.get_basket_content(waitlist.c_id)
    if basket_values is None:
        redis_client.delete(f'basket:{waitlist.c_id}')
    elif basket_values and type(basket_values) == list:
        ch_meth.set_cached_value(redis_client, basket_values, f'basket:{waitlist.c_id}')
    return res


@app.get("/get_waitlist")
async def get_waitlist(c_id: int):
    return db_meth.get_waitlist(c_id)


@app.get("/get_waitlist_seller")
async def get_waitlist_seller(wl_id: int, c_id: int):
    return db_meth.get_waitlist_seller(wl_id, c_id)


@app.post("/upd_basket_qty")
async def upd_basket_qty(c_id: int, basket: models.Basket):
    res = db_meth.upd_basket_qty(basket)
    basket_values = db_meth.get_basket_content(c_id)
    if basket_values:
        ch_meth.set_cached_value(redis_client, basket_values, f'basket:{c_id}')
    return res


@app.post("/add_image")
async def add_image(basket: str, img: models.Image):
    value = image_methods.add_images(basket, [{'item_id': img.item_id, 'is_main': img.is_main, 'base64': img.base64}])
    if basket == 'goods':
        redis_client.delete('goods')
        iso.get_goods_cache()
    return value


@app.post("/fs_approve")
async def fs_approve(type_act: str, result: models.ApproveAction):
    value = db_meth.fs_approve(type_act, result)
    if type_act == 'good':
        redis_client.delete('goods')
        iso.get_goods_cache()
    return value


@app.get("/get_market_status")
async def get_market_status(m_id: int):
    return db_meth.get_market_status(m_id)


@app.get("/get_seller_good_list")
async def get_seller_good_list(c_id: int):
    return db_meth.get_seller_good_list(c_id)


@app.get('/get_cached_good')
async def get_cached_good():
    return iso.get_goods_cache()


@app.get('/search_goods')
async def search_in_goods(query: str = None, cat_ids: str = None, b_ids: str = None):
    dataframe = iso.get_goods_cache()
    if cat_ids is not None:
        cat_ids = [int(i) for i in cat_ids.split(',')]
    if b_ids is not None:
        b_ids = [int(j) for j in b_ids.split(',')]
    return iso.search_dataframe(query, cat_ids, b_ids, dataframe)


@app.post("/copy_to_store")
async def copy_to_store(good: models.MarketStore):
    return db_meth.copy_to_store(good)


@app.get("/get_parent_images_id")
async def get_parent_images_id(g_id: int):
    return db_meth.get_parent_images_id(g_id)


if __name__ == '__main__':
    cfg.con = db.Database().con
    cfg.con_fs = db.FSDatabase().con
    if not cfg.con_fs:
        print("Cannot connect to FS_DB")
        exit()
    if cfg.con:
        if not minio_client.check_health():
            print('Error while connecting to minio')
            exit()
        try:
            redis_client.flushall()
        except ConnectionError:
            print('Error while connecting to redis')
            exit()
        # uvicorn.run(app, host='localhost', port=8000)
        uvicorn.run(app, host=cfg.app_host, port=cfg.app_port)
        redis_client.delete('goods')
        iso.to_cache()
    else:
        print("Cannot connect to DB")
