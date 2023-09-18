import json
import config as cfg
import models
import image_methods
from datetime import datetime
import methods
import re
from init import redis_client
import cache_methods as ch_meth
from collections import defaultdict


def get_values_sql(sql, is_fs: bool = False):
    con = None
    try:
        if is_fs:
            con = cfg.con_fs
        else:
            con = cfg.con
        cur = con.cursor()
        cur.execute(sql)
        column_names = [desc[0] for desc in cur.description]
        value_arr = []
        for record in cur:
            dict_tmp = {}
            for i in range(0, len(column_names)):
                dict_tmp.update({column_names[i].lower(): record[i]})
            value_arr.append(dict_tmp)
        con.commit()
    except Exception as err:
        con.rollback()
        return {'status': -1, 'err_msg': format(err)}

    if all(all(value is None for value in d.values()) for d in value_arr):
        return None

    return value_arr


def insert_values(sql):
    status = 0
    err_msg = None
    con = None
    try:
        con = cfg.con
        cur = con.cursor()
        cur.execute(sql)
    except Exception as err:
        status = -1
        err_msg = format(err)
    finally:
        if status == 0:
            con.commit()
        else:
            try:
                con.rollback()
            except Exception as err:
                print(format(err))
    result = {'status': status}
    if err_msg:
        result.update({'err_msg': err_msg})
    return result


def insert_item(process_name: str, item):
    values = json.loads(item)
    item_name = values['item_name']
    item_code = values['item_code']
    if process_name == 'category':
        return insert_values(f'''insert into CATEGORIES(CAT_CODE, CAT_NAME)
                            values ('{item_code}','{item_name}');''')
    elif process_name == 'subcategory':
        item_parent_id = int(values['parent_id'])
        item_id = int(values['cat_id'])
        if str(item_parent_id).isnumeric():
            insert_values(f'''insert into SUB_CATEGORIES(SC_FROM_CAT_ID, SC_FROM_CAT_ID_PARENT) 
                            values ({item_id},{item_parent_id})''')
            return insert_values(f'''INSERT INTO CATEGORIES(CAT_CODE, CAT_NAME)
                                    VALUES ('{item_code}','{item_name}')''')
        else:
            return {'err_msg': 'Incorrect parent_Id'}
    elif process_name == 'brand':
        cat_id = int(values['cat_id'])
        return insert_values(f'''insert into BRANDS(B_CODE, B_NAME, B_CAT_ID)
                                values ('{item_code}','{item_name}',{cat_id})''')


def get_categories(all_cats: bool = False):
    value_arr = get_values_sql(f'select * from hp_get_categories({all_cats})')
    if value_arr:
        return {"categories": value_arr}
    return None


def get_subcategories(cat_id: int):
    value_arr = get_values_sql(f'select * from hp_get_subcategories({cat_id})')
    if value_arr:
        return {"subcategories": value_arr}
    return None


def get_brands():
    value_arr = get_values_sql('select * from hp_get_active_brands')
    if value_arr:
        return {"brands": value_arr}
    return None


def get_good(g_id: int):
    sql = f'select * from hp_get_good_info({g_id})'
    if redis_client.exists(sql):
        good_info = ch_meth.get_cached_value(redis_client, sql)
    else:
        good_info = get_values_sql(sql)
        ch_meth.set_cached_value(redis_client, good_info, sql)
    if good_info is not None and len(good_info) > 0:
        good_info = good_info[0]
    else:
        good_info = None
    good_main_image = []
    if isinstance(good_main_image, dict) and good_main_image.get('status') == -1:
        return good_main_image
    result = good_info
    if good_main_image:
        result = result.update({'good_image': good_main_image})
    if not result:
        return {"status": 0, "err_msg": "Указанный товар не найден"}
    else:
        return result


def get_goods(quantity: int, last_id: int = 0, params: str = None):
    sql = f"""select * from hp_get_goods_id({quantity}, {last_id}"""
    if params:
        sql += f""", '{params}')"""
    else:
        sql += ')'
    if redis_client.exists(sql):
        return ch_meth.get_cached_value(redis_client, sql)
    g_id_list = get_values_sql(sql)
    if isinstance(g_id_list, dict) and g_id_list.get('status') == -1:
        return g_id_list
    if not g_id_list:
        return None
    result = []
    for g_id in g_id_list:
        result.append(get_good(g_id.get('g_id')))
    return ch_meth.set_cached_value(redis_client, result, sql)


def get_market_store(m_id: int, is_active: bool):
    g_id_list = get_values_sql(f"""select * from hp_get_market_store({m_id}, {is_active})""")
    if isinstance(g_id_list, dict) and g_id_list.get('status') == -1:
        return g_id_list
    if not g_id_list:
        return None
    result = []
    for g_id in g_id_list:
        result.append({**{'m_id': m_id}, **get_good(g_id.get('g_id'))})
    return result


def get_market_store_on_mod(m_id: int):
    g_id_list = get_values_sql(f"""select * from hp_get_market_store_mod({m_id})""")
    if isinstance(g_id_list, dict) and g_id_list.get('status') == -1:
        return g_id_list
    if not g_id_list:
        return None
    result = []
    for g_id in g_id_list:
        result.append(get_good(g_id.get('g_id')))
    return result


def add_good(good):
    print(good)
    return get_values_sql(f"""select g_id from hp_insert_good({good.cat_id}, {good.b_id}, {good.gm_id}, 
                            '{good.gi_memo}', {good.gi_price}, {good.m_id}, '{good.gi_more_ref}')""")


def del_good(pk: models.PrimaryKey):
    return insert_values(f"""execute procedure hp_del_good({pk.id})""")


def de_active_good(action):
    return insert_values(f"""execute procedure hp_de_active_good({action.g_id}, '{action.value}')""")


def get_categories_levels():
    res = {}
    main_cats = get_categories().get('categories')
    for cats in main_cats:
        lst = []
        result = get_subcategories(cats.get('id'))
        if not result:
            continue
        result = result.get('subcategories')
        for i in result:
            lst.append(i.get('id'))
        res.update({cats.get('id'): lst})
    return res


def get_models(cat_id: int = 0, b_id: int = 0):
    value_arr = get_values_sql(f'select * from hp_get_models({cat_id}, {b_id})')
    if value_arr:
        return {"models": value_arr}
    return None


def add_good_to_basket(good):
    return insert_values(f"""execute procedure hp_add_good_to_basket({good.b_id}, {good.g_id}, {good.quantity})""")


def get_basket_content(c_id: int):
    return get_values_sql(f'select * from hp_get_basket_content({c_id})')


def get_token(c_id, user_type, token):
    return get_values_sql(f'''select * from HP_GET_TOKEN({c_id},{user_type},'{token}')''')[0]


def get_brand(b_id: int):
    return get_values_sql(f'select * from brands b where b.b_id = {b_id}')


def insert_new_item(item: models.NewItem):
    if item.type == 'good_model':
        return insert_values(f"""execute procedure hp_insert_new_model('{item.type}', {item.body.get('cat_id')}, 
                                {item.body.get('b_id')}, '{item.body.get('name')}', {item.body.get('c_id')})""")
    if item.type == 'brands':
        return insert_values(f"""execute procedure hp_insert_new_brand('{item.type}', '{item.body.get('name')}', 
                            {item.body.get('c_id')})""")
    if item.type == 'categories':
        return insert_values(f"""execute procedure hp_insert_new_category('{item.type}', '{item.body.get('name')}', 
                            '{item.body.get('parent_id')}', {item.body.get('c_id')})""")


def get_seller_add_apps(c_id: int):
    return get_values_sql(f"""select * from hp_get_seller_add_app({c_id})""")


def get_seller_sell_apps(c_id: int):
    return get_values_sql(f"""select * from hp_get_market_apps({c_id})""")


def delete_good_from_basket(c_id, g_id):
    return insert_values(f'''execute procedure hp_del_basket_good({g_id},{c_id})''')


def clear_basket(c_id):
    return insert_values(f'''execute procedure hp_clear_basket({c_id})''')


def get_seller_markets(c_id: int):
    return get_values_sql(f'''select * from hp_get_seller_markets({c_id})''')


def upd_basket_qty(basket: models.Basket):
    return insert_values(f'''execute procedure hp_upd_quantity_in_basket({basket.b_id}, {basket.g_id}, 
                        {basket.quantity})''')


def create_waitlist(waitlist):
    c_id = waitlist.c_id
    obj_kind = waitlist.obj_kind
    obj_id_list = waitlist.obj_id_list
    obj_id = None
    if len(obj_id_list) == 1:
        obj_id = obj_id_list[0]
        res = get_values_sql(f'''select wl_id from hp_add_item_waitlist({c_id}, {obj_id}, '{obj_kind}')''')
        if type(res) != list:
            return {'status': -1, 'err_msg': res.get('err_msg')}
    else:
        for g_id in obj_id_list:
            res = get_values_sql(f'''select wl_id from hp_add_item_waitlist({c_id}, {g_id}, '{obj_kind}')''')
            if type(res) != list:
                return {'status': -1, 'err_msg': res.get('err_msg')}

    return {'status': 0}


def parent_category(cat_id: int):
    res = get_values_sql(f'''select * from hp_get_parent_category({cat_id})''')
    if res:
        return res[0]
    return res


def get_app_result(al_id: int):
    res = get_values_sql(f'''select * from hp_get_moderator_comment({al_id})''')
    if res:
        return res[0]
    return res


def red_good(good: models.Good):
    res = insert_values(f"""execute procedure hp_red_good_info({good.g_id}, {good.cat_id}, {good.b_id}, 
                        '{good.gi_memo}', {good.gi_price}, {good.gm_id}, '{good.gi_more_ref}')""")
    if res.get('status') != 0:
        return res
    if good.images:
        return image_methods.add_images(basket='goods', images=good.images)
    return res


def get_notifications(c_id: int):
    return get_values_sql(f"""select * from hp_get_notifications({c_id})""")


def get_unread_notifications(c_id: int):
    return get_values_sql(f"""select * from hp_get_unread_notifications({c_id})""")


def read_notification(notification: models.Notifications):
    return insert_values(f'''execute procedure hp_read_notification({notification.n_id})''')


def get_waitlist(c_id: int):
    result_tmp = get_values_sql(f"""select * from hp_get_waitlist({c_id})""")
    if not result_tmp or len(result_tmp) == 0:
        return []
    transformed_data = defaultdict(lambda: defaultdict(list))

    for item in result_tmp:
        wl_id = item.get('wl_id', None)
        m_id = item.get('m_id', None)
        is_locked = item.get('is_locked', None)

        if wl_id is not None and m_id is not None:
            transformed_data[(wl_id, is_locked)][m_id].append(item)

    output = []
    keys = ['m_id', 'wl_id', 'mi_name', 'is_locked']
    for (wl_id, is_locked), markets_data in transformed_data.items():
        markets = []
        for m_id, goods in markets_data.items():
            mi_name = goods[0].get('mi_name', None)
            for good in goods:
                for key in keys:
                    good.pop(key, None)
            markets.append({'m_id': m_id, 'mi_name': mi_name, 'goods': goods})
        output.append({'wl_id': wl_id, 'is_locked': is_locked, 'markets': markets})
    return output


def get_waitlist_id(c_id: int):
    result = get_values_sql(f"""select * from hp_get_waitlist_id({c_id});""")
    if result:
        return result[0]


def get_goods_by_wl_id(wl_id: int):
    return get_values_sql(f"""select * from hp_get_good_by_wl_id({wl_id})""")


def get_waitlist_seller(wl_id: int, c_id: int):
    return get_values_sql(f"""select * from hp_get_waitlist_seller({wl_id}, {c_id})""")


def market_good_from_wl(m_id: int, g_id_string: str):
    return get_values_sql(f"""select * from hp_get_goods_from_wait_list('{m_id}', '{g_id_string}')""")


def get_market_info(m_id: int):
    result = get_values_sql(f"""select * from hp_get_market_info({m_id})""")
    if result:
        try:
            result = result[0]
            input_list = result.get('working_days').split(",")
            result_list = [False] * 7
            for num_str in input_list:
                num = int(num_str)
                result_list[num - 1] = True
            time_open = result.get('time_open')
            if time_open:
                result.update({"time_open": methods.time_str_to_date_picker(time_open)})
            time_close = result.get('time_close')
            if time_close:
                result.update({"time_close": methods.time_str_to_date_picker(time_close)})
            result.update({"working_days": result_list})
            return [result]
        except Exception as err:
            return {"status": -1, "err_msg": format(err)}
    return result


def get_market_contacts(m_id: int):
    return get_values_sql(f"""select * from hp_get_market_contacts({m_id})""")


def redact_market_info(mi: models.MarketInfo):
    working_days_indices = [str(index + 1) for index, value in enumerate(mi.mi_working_days) if value]
    result_string = ','.join(working_days_indices)
    time_open = f'{datetime.strptime(mi.mi_time_open, "%H:%M").time()}' if mi.mi_time_open else None
    time_close = f'{datetime.strptime(mi.mi_time_close, "%H:%M").time()}' if mi.mi_time_close else None
    m_id = mi.m_id if mi.m_id else 'null'
    mi.mi_latitude = mi.mi_latitude if mi.mi_latitude else 'null'
    mi.mi_longitude = mi.mi_longitude if mi.mi_longitude else 'null'
    ci_id = mi.mi_from_ci_id
    if not ci_id:
        tmp = get_values_sql(f'''select * from hp_find_or_insert_city('{mi.city}')''')
        ci_id = tmp[0].get('ci_id')
    if time_open:
        return get_values_sql(f'''select m_id from hp_red_market_info({mi.c_id}, {mi.mi_reg}, '{mi.mi_name}',
                            '{result_string}', '{time_open}', '{time_close}', {mi.mi_atc}, '{mi.mi_address}',
                            {mi.mi_latitude}, {mi.mi_longitude}, {ci_id}, {m_id})''')
    else:
        return get_values_sql(f'''select m_id from hp_red_market_info({mi.c_id}, {mi.mi_reg}, '{mi.mi_name}',
                                    '{result_string}', null, null, {mi.mi_atc}, '{mi.mi_address}',
                                    {mi.mi_latitude}, {mi.mi_longitude}, {ci_id}, {m_id})''')


def redact_market_contacts(mc):
    ct = get_values_sql("select ct_id, ct_code from hp_get_contact_types")
    m_id = mc.m_id

    def insert_upd_action(item, keys_list):
        ct_id = None
        mc_info = None
        mc_id = None
        for key in keys_list:
            if key != 'mc_id':
                ct_id = next((item['ct_id'] for item in ct if item['ct_code'] == key), None)
                mc_info = item.get(key)
            else:
                mc_id = item.get('mc_id')
        if any([mc_info, mc_id]):
            if mc_id:
                insert_values(f"""execute procedure hp_ins_upd_m_contacts({m_id}, {ct_id}, '{mc_info}', {mc_id})""")
            else:
                insert_values(f"""execute procedure hp_ins_upd_m_contacts({m_id}, {ct_id}, '{mc_info}')""")

    try:
        for contact in mc.contacts:
            keys = list(contact.keys())
            if len(keys) > 1:
                insert_upd_action(contact, keys)
            else:
                if len(contact.get(keys[0])) > 0:
                    for el in contact.get(keys[0]):
                        keys_tmp = list(el.keys())
                        insert_upd_action(el, keys_tmp)
    except Exception as err:
        return {"status": -1, "err_msg": format(err)}
    return {"status": 0}


def get_all_goods():
    return get_values_sql(f"""select g_id, gm_name, cat_name, b_name, gi_memo, gi_price, g_create_datetime, m_id, ci_id, mi_lat, mi_lon, 
                                g_from_cat_id, g_from_b_id, gi_from_gm_id from hp_get_all_active_goods""")


def create_miniapp(c_id):
    value = get_values_sql(f'''select * from hp_miniapp_from_waitlist({c_id})''')[0]
    if value.get('create_status') == 1:
        return {'status': 0, 'err_msg': 'Miniapp created successfully.'}
    else:
        return {'status': -1, 'err_msg': 'Cannot create miniapp.'}


def fs_approve(type_act: str, result: models.ApproveAction):
    procedure = ''
    if type_act == 'good':
        procedure = 'hp_approve_good'
    if type_act == 'market':
        procedure = 'hp_approve_market'
    if type_act in ['category', 'brand', 'model']:
        procedure = 'hp_approve_sort_types'
    if procedure == '':
        return {'status': -1, 'err_msg': 'Unknown procedure'}
    return get_values_sql(f"""select * from {procedure}({result.id}, {result.al_id}, {result.res_status}, 
                            {result.c_id}, '{result.memo}')""")


def get_market_status(m_id: int):
    return get_values_sql(f"""select * from hp_get_market_status({m_id})""")


def get_seller_good_list(c_id: int):
    brands_list = []
    cat_list = []
    brands = None
    categories = None
    markets = get_values_sql(f"""select m_id as id, name from hp_get_seller_markets({c_id})""")
    res = get_values_sql(f"""select * from hp_get_seller_good_list({c_id})""")
    if res:
        for item in res:
            if not item.get('b_id') in brands_list:
                brands_list.append(item.get('b_id'))
            if not item.get('cat_id') in cat_list:
                cat_list.append(item.get('cat_id'))

        brands = get_values_sql(f"""select b_id as id, b_name as name from brands where b_id in 
                ({', '.join(str(x) for x in brands_list)})""")
        categories = get_values_sql(f"""select cat_id  as id, cat_name as name from categories where cat_id in 
                ({', '.join(str(x) for x in cat_list)})""")

    return {'markets': markets, 'goods': res, 'brands': brands, 'categories': categories}


def copy_to_store(good: models.MarketStore):
    return insert_values(f"""execute procedure hp_copy_good_to_store({good.m_id}, {good.g_id})""")


def get_parent_images_id(g_id: int):
    return get_values_sql(f"""select f_get_parent_img_id({g_id}, 'goods') from rdb$database""")


def change_good_status_in_wl(params):
    return get_values_sql(f'select * from hp_approve_good_in_waitlist(\'{params}\')', False)


def get_city_name(ci_id: int):
    return get_values_sql(f"""select * from hp_find_city_name({ci_id})""")


def seller_answer_deal(deal: models.Deal):
    try:
        for good in deal.result:
            insert_values(f"""update deal_info set di_good_qty = {good.get('g_qty')}, 
                        di_seller_status = {good.get('status')} where(di_from_d_id= {deal.d_id} and 
                        di_from_g_id = {good.get('g_id')})""")
        counter = get_values_sql(f"""select count(*) from deal_info di where di.di_from_d_id = {deal.d_id} and 
            di.di_seller_status = 0""")
        if counter[0].get('count') == 0:
            counter = get_values_sql(f"""select count(*) from deal_info di where di.di_from_d_id = 26 and 
                                    di.di_seller_status > -2""")
            if counter[0].get('count') > 0:
                status = 1
            else:
                status = -1
            insert_values(f"""update deals set d_status = {status} where (d_id = {deal.d_id})""")
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}
    return {'status': 0}


def parents_cats():
    return get_values_sql("select * from hp_get_parents_cats")


def cities():
    return get_values_sql("select * from hp_get_cities")


def client_city(client: models.Client):
    try:
        insert_values(f"""execute procedure hp_upd_ins_client_city({client.c_id}, {client.ci_id})""")
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}
    return {'status': 0}


def find_city(city_name: str):
    if bool(re.fullmatch(r'[-а-яёА-ЯЁ]*', city_name)):
        return get_values_sql(f'''select * from hp_find_or_insert_city('{city_name}')''')
    else:
        return {'status': -1, 'err_msg': 'Недопустимые символы'}


def waitlist_client_result(result: models.WaitlistCliResult):
    if result.results and len(result.results) > 0:
        counter = get_values_sql(f'''select count(*) from waitlist wl where wl_id = {result.wl_id} 
                                            and wl_status not in (2, -2)''')
        if counter:
            counter = counter[0].get('count')
            if counter == 0:
                return {'status': -1, 'err_msg': 'Все результаты уже получены'}
            else:
                for result_tmp in result.results:
                    insert_values(f'''update waitlist set wl_status = {result_tmp.get('status')} 
                                        where wl_id = {result.wl_id} and wl_from_g_id = {result_tmp.get('g_id')}''')
                if counter == len(result.results):
                    insert_values(f'''update cli_waitlist set clw_is_locked = true 
                                        where(clw_from_wl_id = {result.wl_id})''')
                d_id = get_values_sql(f"""select d_id from hp_new_deal({result.wl_id}, 0, {result.pay_type}, 
                                    {result.delivery_type})""")

                if d_id and d_id[0].get('d_id') > 0:
                    d_id = d_id[0].get('d_id')
                    for result_tmp in result.results:
                        insert_values(f"""execute procedure hp_new_deal_info({d_id}, {result_tmp.get('g_id')}, 
                        {result_tmp.get('qty')}, 0)""")

                    return {'status': 0}
                else:
                    return {'status': -1, 'err_msg': 'Произошла ошибка'}
    else:
        return {'status': -1, 'err_msg': 'Результат не может быть пустым'}


def client_deal(c_id: int):
    res = get_values_sql(f"""select * from hp_client_deal({c_id})""")
    if not res:
        return None
    result = []

    d_id_dict = {}

    for item in res:
        d_id = item['d_id']
        m_id = item['m_id']
        goods_info = {'g_id': item['g_id'], 'g_qty': item['g_qty'], 'seller_status': item['seller_status'],
                      'client_status': item['client_status']}

        if d_id not in d_id_dict:
            d_id_dict[d_id] = {}

        if 'markets' not in d_id_dict[d_id]:
            d_id_dict[d_id]['markets'] = {}

        if m_id not in d_id_dict[d_id]['markets']:
            d_id_dict[d_id]['markets'][m_id] = {'goods': []}

        d_id_dict[d_id]['markets'][m_id]['goods'].append(goods_info)

    for d_id, d_info in d_id_dict.items():
        transformed_item = {'d_id': d_id, 'markets': []}

        for m_id, m_info in d_info['markets'].items():
            market_info = {'m_id': m_id, 'goods': m_info['goods']}
            transformed_item['markets'].append(market_info)

        result.append(transformed_item)
    return result


def post_review(review: models.Rait):
    return insert_values(f"""execute procedure hp_post_review({review.c_id}, '{review.rait_type}', {review.id}, 
                    {review.rait}, '{review.review}')""")


def get_market_rait(m_id: int):
    sql = f"""select rait from hp_get_market_rait({m_id})"""
    if redis_client.exists(sql):
        return ch_meth.get_cached_value(redis_client, sql)
    value = get_values_sql(sql)
    if value:
        return ch_meth.set_cached_value_by_minutes(redis_client, value, sql, 15)


def get_reviews(obj_id: int, table_name: str):
    sql = f"""select * from hp_get_reviews({obj_id}, '{table_name}')"""
    if redis_client.exists(sql):
        return ch_meth.get_cached_value(redis_client, sql)
    value = get_values_sql(sql)
    if value:
        return ch_meth.set_cached_value_by_minutes(redis_client, value, sql, 15)
