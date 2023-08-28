import json
import pickle
import database_methods as db_mth
import fs_database_methods as fs_dm
import pandas as pd
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


def search_dataframe(query: str = None, cat_ids=None, b_ids=None, dataframe=None):
    """

    :param cat_ids:
    :param b_ids:
    :param dataframe
    :type query: object
    """
    dataframe = pd.DataFrame.from_records(dataframe)
    search_cols = ['GI_MEMO', 'GM_NAME', 'B_NAME', 'CAT_NAME']
    results = []

    if dataframe is None:
        raise ValueError("You need to provide a DataFrame.")

    filtered_df = dataframe.copy()

    if cat_ids:
        filtered_df = filtered_df[filtered_df['g_from_cat_id'].isin(cat_ids)]

    if b_ids:
        filtered_df = filtered_df[filtered_df['g_from_b_id'].isin(b_ids)]

    if query:
        query = query.lower()
        for col in search_cols:
            col_lower = col.lower()
            matched_rows = filtered_df[filtered_df[col_lower].str.contains(query, case=False, na=False)]
            results.extend(matched_rows.to_dict(orient='records'))
    else:
        results = filtered_df.to_dict(orient='records')

    return results
