import json
import pickle
import database_methods as db_mth
import fs_database_methods as fs_dm
import pandas as pd

import methods
from main import redis_client


def to_cache():
    regions = get_all_regions()
    goods = get_goods_cache()


def get_all_regions():
    return fs_dm.get_regions(1000000)


def get_goods_cache():
    cached_data = redis_client.get('goods')
    if cached_data:
        return json.loads(cached_data.decode('utf-8'))  # Return cached JSON directly

    data_frame = db_mth.get_all_goods()
    cols = list(data_frame[0].keys())
    df = pd.DataFrame(data_frame, columns=cols)
    serialized_data = df.to_json(orient='records')
    redis_client.set('goods', serialized_data)

    return json.loads(serialized_data)


def search_dataframe(sort_options=None, query=None, cat_ids=None, b_ids=None, gm_ids_arr=None, price=None,
                     dataframe=None, points=None, ci_id=None):
    """
    :param ci_id:
    :param points:
    :param sort_options:
    :param price:
    :param cat_ids:
    :param b_ids:
    :param dataframe
    :param gm_ids_arr
    :type query: object
    """
    dataframe = pd.DataFrame.from_records(dataframe)
    search_cols = ['GI_MEMO', 'GM_NAME', 'B_NAME', 'CAT_NAME']
    results = []

    if dataframe is None:
        raise ValueError("You need to provide a DataFrame.")

    filtered_df = dataframe.copy()

    filtered_df = filtered_df[filtered_df['ci_id'].isin([ci_id])]

    if cat_ids:
        filtered_df = filtered_df[filtered_df['g_from_cat_id'].isin(cat_ids)]

    if b_ids:
        filtered_df = filtered_df[filtered_df['g_from_b_id'].isin(b_ids)]

    if gm_ids_arr:
        filtered_df = filtered_df[filtered_df['gi_from_gm_id'].isin(gm_ids_arr)]

    if isinstance(price, dict):
        min_price = price.get('low_price')
        if not min_price:
            min_price = 0
        max_price = price.get('max_price')
        if max_price and max_price > 0:
            filtered_df = filtered_df[(filtered_df['gi_price'] >= min_price) &
                                      (filtered_df['gi_price'] <= max_price)]
        else:
            filtered_df = filtered_df[(filtered_df['gi_price'] >= min_price)]
    if query:
        if isinstance(query, str):
            query = query.lower()
            for col in search_cols:
                col_lower = col.lower()
                matched_rows = filtered_df[filtered_df[col_lower].str.contains(query, case=False, na=False)]
                results.extend(matched_rows.to_dict(orient='records'))
    else:
        results = filtered_df.to_dict(orient='records')

    if points:
        filtered_df['distance'] = filtered_df.apply(
            lambda row: methods.calculating_distance((float(row.get('mi_lat')), float(row.get('mi_lon'))),
                                                     (float(points.get('lat')),
                                                      float(points.get('lon')))), axis=1)
        results = filtered_df.sort_values(by='distance', ascending=True)

    if sort_options:
        if sort_options.get('price'):
            results = filtered_df.sort_values(by='gi_price', ascending=sort_options.get('price'))
        if sort_options.get('date'):
            results = filtered_df.sort_values(by='g_create_datetime', ascending=sort_options.get('date'))

        results = filtered_df.to_dict(orient='records')
    return results
