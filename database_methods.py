import json
from init import app
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
            con = app.con_fs
        else:
            con = app.con
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
        con = app.con
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
    value_arr = ch_meth.set_or_get_cached_sql(f'select * from hp_get_categories({all_cats})', 15)
    if value_arr:
        return {"categories": value_arr}
    return None


def get_subcategories(cat_id: int):
    value_arr = ch_meth.set_or_get_cached_sql(f'select * from hp_get_subcategories({cat_id})', 15)
    if value_arr:
        return {"subcategories": value_arr}
    return None


def get_brands():
    value_arr = ch_meth.set_or_get_cached_sql('select * from hp_get_active_brands', 15)
    if value_arr:
        return {"brands": value_arr}
    return None


def get_good(g_id: int):
    sql = f'select * from hp_get_good_info({g_id})'
    good_info = ch_meth.set_or_get_cached_sql(sql, 15)
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
        return ch_meth.get_cached_value(sql)
    g_id_list = get_values_sql(sql)
    if isinstance(g_id_list, dict) and g_id_list.get('status') == -1:
        return g_id_list
    if not g_id_list:
        return None
    result = []
    for g_id in g_id_list:
        result.append(get_good(g_id.get('g_id')))
    return ch_meth.set_cached_value_by_minutes(result, sql, 15)


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
    value_arr = ch_meth.set_or_get_cached_sql(f'select * from hp_get_models({cat_id}, {b_id})', 15)
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
    return ch_meth.set_or_get_cached_sql(f'select * from brands b where b.b_id = {b_id}', 15)


def get_sql_query(item_type: str, **kwargs) -> str:
    sql_templates = {
        'good_model': "execute procedure hp_insert_new_model('{item_type}', {cat_id}, {b_id}, '{name}', {c_id})",
        'brands': "execute procedure hp_insert_new_brand('{item_type}', '{name}', {c_id})",
        'categories': "execute procedure hp_insert_new_category('{item_type}', '{name}', '{parent_id}', {c_id})"
    }
    return sql_templates.get(item_type).format(item_type=item_type, **kwargs)


def insert_new_item(item: models.NewItem):
    query = get_sql_query(item.type, **item.body)
    return insert_values(query)


def get_seller_add_apps(c_id: int):
    return ch_meth.set_or_get_cached_sql(f"""select * from hp_get_seller_add_app({c_id})""", 1)


def get_seller_sell_apps(c_id: int):
    return ch_meth.set_or_get_cached_sql(f"""select * from hp_get_market_apps({c_id})""", 1)


def delete_good_from_basket(c_id, g_id):
    return insert_values(f'''execute procedure hp_del_basket_good({g_id},{c_id})''')


def clear_basket(c_id):
    return insert_values(f'''execute procedure hp_clear_basket({c_id})''')


def get_seller_markets(c_id: int):
    return ch_meth.set_or_get_cached_sql(f'''select * from hp_get_seller_markets({c_id})''', 1)


def upd_basket_qty(basket: models.Basket):
    return insert_values(f'''execute procedure hp_upd_quantity_in_basket({basket.b_id}, {basket.g_id}, 
                        {basket.quantity})''')


def create_waitlist(waitlist):
    c_id = waitlist.c_id
    obj_kind = waitlist.obj_kind
    obj_id_list = waitlist.obj_id_list
    if obj_kind != 'deals':
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
    else:
        obj_id = waitlist.obj_id_list[0]
        res = get_values_sql(f'''select wl_id from hp_repeat_deal({c_id}, {obj_id})''')
        if type(res) != list:
            return {'status': -1, 'err_msg': res.get('err_msg')}

    return {'status': 0}


def parent_category(cat_id: int):
    res = ch_meth.set_or_get_cached_sql(f'''select * from hp_get_parent_category({cat_id})''', 15)
    if res:
        return res[0]
    return res


def get_app_result(al_id: int):
    res = ch_meth.set_or_get_cached_sql(f'''select * from hp_get_moderator_comment({al_id})''', 15)
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
    return get_values_sql(f"""select * from hp_get_waitlist_id({c_id});""")


def get_goods_by_wl_id(wl_id: int):
    return ch_meth.set_or_get_cached_sql(f"""select * from hp_get_good_by_wl_id({wl_id})""", 15)


def get_waitlist_seller(wl_id: int, c_id: int):
    return get_values_sql(f"""select * from hp_get_waitlist_seller({wl_id}, {c_id})""")


def market_good_from_wl(m_id: int, g_id_string: str):
    return get_values_sql(f"""select * from hp_get_goods_from_wait_list({m_id}, '{g_id_string}')""")


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
    return ch_meth.set_or_get_cached_sql(f"""select g_id, gm_name, cat_name, b_name, gi_memo, gi_price, 
                                        g_create_datetime, m_id, ci_id, mi_lat, mi_lon, g_from_cat_id, g_from_b_id, 
                                        gi_from_gm_id from hp_get_all_active_goods""", 15)


def create_miniapp(c_id):
    value = get_values_sql(f'''select * from hp_miniapp_from_waitlist({c_id})''')[0]
    if value.get('create_status') == 1:
        return {'status': 0, 'err_msg': 'Miniapp created successfully.'}
    else:
        return {'status': -1, 'err_msg': 'Cannot create miniapp.'}


def fs_approve(type_act: str, result: models.ApproveAction):
    procedure_dict = {
        'good': 'hp_approve_good',
        'market': 'hp_approve_market',
        'category': 'hp_approve_sort_types',
        'brand': 'hp_approve_sort_types',
        'model': 'hp_approve_sort_types',
        'review': 'hp_approve_review'
    }
    procedure = procedure_dict.get(type_act)
    if not procedure:
        return {'status': -1, 'err_msg': 'Unknown procedure'}
    return get_values_sql(f"""select * from {procedure}({result.id}, {result.al_id}, {result.res_status}, 
                            {result.c_id}, '{result.memo}')""")


def get_market_status(m_id: int):
    return ch_meth.set_or_get_cached_sql(f"""select * from hp_get_market_status({m_id})""", 15)


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
    return ch_meth.set_or_get_cached_sql(f"""select f_get_parent_img_id({g_id}, 'goods') from rdb$database""", 15)


def change_good_status_in_wl(params):
    return get_values_sql(f'select * from hp_approve_good_in_waitlist(\'{params}\')', False)


def get_city_name(ci_id: int):
    return ch_meth.set_or_get_cached_sql(f"""select * from hp_find_city_name({ci_id})""", 15)


def seller_answer_deal(deal: models.Deal):
    try:
        res = get_values_sql(f"""select cwl.clw_from_c_id as c_id, d.d_pay_type as pay_type from cli_waitlist cwl inner 
                                    join deals d on d.d_from_wl_id = cwl.clw_from_wl_id and d.d_id = {deal.d_id}""")[0]

        for good in deal.result:
            insert_values(f"""update deal_info set di_good_qty = {good.g_qty}, 
                        di_seller_status = {good.status} where(di_from_d_id= {deal.d_id} and 
                        di_from_g_id = {good.g_id})""")
        counter = get_values_sql(f"""select count(*) from deal_info di where di.di_from_d_id = {deal.d_id} and 
            di.di_seller_status = 0""")
        if counter[0].get('count') == 0:
            counter = get_values_sql(f"""select count(*) from deal_info di where di.di_from_d_id = 26 and 
                                    di.di_seller_status > -2""")
            if counter[0].get('count') > 0:
                status = 2 if res.get('pay_type') != 3 else 1
            else:
                status = -2 if res.get('pay_type') != 3 else -1
            insert_values(f"""update deals set d_status = {status} where (d_id = {deal.d_id})""")
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}
    value = {"need_confirm": res.get('pay_type') == 3, "type": res.get('pay_type')}
    ch_meth.set_cached_value(value, f"""deal_complete:{res.get("c_id")}:{deal.d_id}""")
    return {'status': 0}


def parents_cats():
    return ch_meth.set_or_get_cached_sql("select * from hp_get_parents_cats", 15)


def cities():
    return ch_meth.set_or_get_cached_sql("select * from hp_get_cities", 15)


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
        counter = get_values_sql(f'''select count(*) from waitlist wl where wl_id = {result.wl_id} and
            wl_status not in (-2, 2)''')
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
    d_id_dict = {}
    for item in res:
        d_id = item['d_id']
        m_id = item['m_id']

        if d_id not in d_id_dict:
            d_id_dict[d_id] = {
                'd_id': d_id,
                'd_status': item['d_status'],
                'deal_info': {
                    'pay_type': item['pay_type'],
                    'pay_type_str': item['pay_type_str'],
                    'delivery_type': item['delivery_type'],
                    'delivery_type_str': item['delivery_type_str']
                },
                'markets': {}
            }
        if m_id not in d_id_dict[d_id]['markets']:
            d_id_dict[d_id]['markets'][m_id] = {'m_id': m_id, 'goods': []}
        goods_info = {
            'g_id': item['g_id'],
            'g_qty': item['g_qty'],
            'seller_status': item['seller_status'],
            'client_status': item['client_status']
        }
        d_id_dict[d_id]['markets'][m_id]['goods'].append(goods_info)
    result = []
    for d_info in d_id_dict.values():
        transformed_item = {
            'd_id': d_info['d_id'],
            'd_status': d_info['d_status'],
            'deal_info': d_info['deal_info'],
            'markets': list(d_info['markets'].values())
        }
        result.append(transformed_item)
    return result


def post_review(review: models.Rait):
    sql = f"""execute procedure hp_post_review({review.c_id}, '{review.rait_type}', {review.id}, {review.rait}"""
    sql += f", {review.review}" if review.review else ''
    sql += ")"
    return insert_values(sql)


def get_market_rait(m_id: int):
    return ch_meth.set_or_get_cached_sql(f"""select rait from hp_get_market_rait({m_id})""", 15)


def get_reviews(obj_id: int, table_name: str):
    return ch_meth.set_or_get_cached_sql(f"""select * from hp_get_reviews({obj_id}, '{table_name}')""", 15)


def get_deals_history(c_id: int, params: str = 'null'):
    sql = f"""select * from hp_get_deals_history({c_id}, '{params}')"""
    if redis_client.exists(sql):
        return ch_meth.get_cached_value(sql)
    value = get_values_sql(sql)
    if not value:
        return []
    d_id_list = []
    result = []
    for deal in value:
        d_id = deal.get('d_id')
        if d_id not in d_id_list:
            d_id_list.append(d_id)
            result.append({"d_id": deal.get("d_id"), "d_datetime": deal.get("d_datetime"),
                           "d_delivery_type": deal.get("d_delivery_type"), "delivery_str": deal.get("delivery_str"),
                           "d_pay_type": deal.get("d_pay_type"), "pay_str": deal.get("pay_str"),
                           "goods": [{"g_id": deal.get("g_id"), "qty": deal.get("di_good_qty"),
                                      "hist_price": deal.get("hist_price")}]})
        else:
            for res in result:
                if res.get("d_id") == d_id:
                    res.get("goods").append({"g_id": deal.get("g_id"), "qty": deal.get("di_good_qty"),
                                             "hist_price": deal.get("hist_price")})
                    break
    if value:
        return ch_meth.set_cached_value_by_minutes(result, sql, 15)


def client_answer_deal(data):
    value = get_values_sql(f"""select * from hp_client_answer_deal('{data}')""")
    if isinstance(value, list):
        return value[0]
    else:
        return {'status': -1, 'err_msg': 'Произошла ошибка'}


def get_active_markets():
    return ch_meth.set_or_get_cached_sql("select * from hp_get_active_markets", 15)


def post_report(report: models.Report):
    try:
        return insert_values(f"""execute procedure hp_ins_report({report.r_type}, '{report.r_message}', 
                            {report.c_id})""")
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}
