import io
import re
from minio import Minio, S3Error
from urllib3.exceptions import MaxRetryError
import config as cfg


class CustomMinio:
    def __init__(self, secure=True):
        self.client = Minio(cfg.minio_endpoint,
                            access_key=cfg.minio_user, secret_key=cfg.minio_password, secure=secure)

    def list_buckets(self):
        return self.client.list_buckets()

    def delete_file(self, basket, item_id, object_name):
        try:
            self.client.remove_object(bucket_name=basket, object_name=f'{item_id}/{object_name}')
        except S3Error:
            return {'status': -1, "err_msg": "Error while deleting file"}
        else:
            return {'status': 0, "err_msg": "File succesfully deleted"}

    def get_folder_image_last(self, basket, item_id):
        objects = self.client.list_objects(bucket_name=basket, prefix=f'{item_id}/', recursive=False)
        objname_list = [i.object_name for i in objects]
        max_number = -1
        for item in objname_list:
            match = re.search(r'\d+/item_(\d+)\.\w+', item)
            if match:
                number = int(match.group(1))
                max_number = max(max_number, number)
        return max_number

    def get_folder_image_count(self, basket, item_id):
        objects = self.client.list_objects(bucket_name=basket, prefix=f'{item_id}/', recursive=False)
        return sum([1 for i in objects])

    def get_goods_images(self, g_id, only_main, bucket_name):
        objects = self.client.list_objects(bucket_name=bucket_name, prefix=f'{g_id}/', recursive=False)
        urls = []
        for item in objects:
            temp_dict = {
                "g_id": g_id,
                "image_ref": self.client.presigned_get_object(bucket_name=bucket_name, object_name=item.object_name),
                "is_main": 1 if 'item_0' in item.object_name else 0}
            if only_main == 1 and temp_dict['is_main'] == 1:
                urls.append(temp_dict)
            elif only_main == 0 and temp_dict['is_main'] == 0:
                urls.append(temp_dict)
        return urls

    def create_folder(self, bucket_name, folder_name):
        objects = self.client.list_objects(bucket_name='goods', prefix=folder_name, recursive=False)
        if not any(objects):
            return self.client.put_object(bucket_name, folder_name, io.BytesIO(b''), 0)
        return None

    def upload_file(self, bucket_name, file_path, object_name, file_len):
        self.client.put_object(bucket_name, object_name, file_path, file_len)
        return self.client.presigned_get_object(bucket_name, object_name)

    def check_health(self):
        try:
            self.client.list_buckets()
            return True
        except MaxRetryError:
            return False
