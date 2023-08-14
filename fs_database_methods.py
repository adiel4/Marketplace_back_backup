import database_methods as db_meth


def get_client_info(c_id: int):
    sql = f"""select c.c_name_fio, c.c_birth_date from clients c where c.c_id = {c_id}"""
    res = db_meth.get_values_sql(sql, True)[0]
    sql = f"""select b_id from hp_get_basket_id({c_id})"""
    res_b_id = db_meth.get_values_sql(sql)[0]
    sql = f"""select m_id from markets where m_owner = {c_id}"""
    res_m_id = db_meth.get_values_sql(sql)[0]
    result = {**res, **res_b_id}
    if res_m_id:
        result = {**result, **res_m_id}
    return result


def get_regions(reg_id: int = 1000000):
    sql = f"""select id_list, id_parent, parent_name, reg.reg_name
            from region_list({reg_id}, 1, 3, 1) rl
            inner join region reg on rl.id_list = reg.reg_unicode
            order by id_parent"""
    res = db_meth.get_values_sql(sql, True)

    tmp = {}
    result = []
    parent_id = 0
    for item in res:
        tmp_parent = item.get("id_parent")
        if tmp_parent != parent_id:
            if tmp != {}:
                result.append(tmp)
            parent_id = tmp_parent
            parent_name = item.get("parent_name")
            tmp = {tmp_parent: {"parent_name": parent_name, "regions": []}}
        else:
            tmp.get(tmp_parent).get("regions").append({"reg_id": item.get("id_list"), "reg_name": item.get("reg_name")})
    return result
