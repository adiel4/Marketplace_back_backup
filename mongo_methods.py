import datetime
import hashlib
from init import app


def ins_or_upd_error_front(post=dict, is_back: bool = False):
    current_date = datetime.datetime.now().strftime('%d.%m.%Y')
    current_time = datetime.datetime.now().strftime('%H:%M:%S')
    post_str = str(post)
    post_hash = hashlib.md5(post_str.encode('utf-8')).hexdigest()

    error = {post_hash: {**post, "time": [current_time]}}

    target_collection = getattr(app.db_errors, 'back' if is_back else 'front')

    existing_document = target_collection.find_one({'_id': current_date})

    if existing_document:
        error_index = next((index for index, e in enumerate(existing_document["errors"]) if post_hash in e), None)
        if error_index is not None:
            key_to_update = f"errors.{error_index}.{post_hash}.time"
            target_collection.update_one({'_id': current_date}, {'$push': {key_to_update: current_time}})
        else:
            target_collection.update_one({'_id': current_date}, {'$push': {'errors': error}})
    else:
        target_collection.insert_one({'_id': current_date, 'errors': [error]})
