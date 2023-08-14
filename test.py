market_goods = [{'g_id': 6}, {'g_id': 7}]
datalist = [{'g_id': 6, 'g_name': 'HW60-BP10929A', 'g_qty': 2}, {'g_id': 7, 'g_name': 'iPhone 13 4/128GB', 'g_qty': 1}]
g_id_values = [int(item['g_id']) for item in datalist]
for item in market_goods:
    a = item['g_id']
    print(a)
    print(g_id_values.index(a))