import urllib.parse as parser
import imghdr
import base64
from io import BytesIO
import models
from main import minio_client
import io
from PIL import Image


def compress_image(image, desired_size):
    original_image_io = BytesIO()
    try:
        image.save(original_image_io, format=image.format)
        original_image_data = original_image_io.getvalue()

        if len(original_image_data) <= desired_size * 1024:
            return original_image_data

        compression_ratio = (desired_size * 1024) / len(original_image_data)

        new_width = int(image.width * compression_ratio)
        new_height = int(image.height * compression_ratio)

        compressed_image = image.resize((new_width, new_height), Image.ANTIALIAS)

        compressed_image_io = BytesIO()
        try:
            compressed_image.save(compressed_image_io, format=image.format)
            return compressed_image_io.getvalue()
        finally:
            compressed_image_io.close()
    finally:
        original_image_io.close()


def add_images(basket, images: list, size: int = 300):
    is_list = True if any(not isinstance(item, models.Image) for item in images) else False
    if not is_list:
        return {'status': -1, 'err_msg': 'Invalid file type in list.'}
    elif len(images) + minio_client.get_folder_image_count(basket, images[0]['item_id']) > 10:
        return {'status': -1, 'err_msg': 'Too many items'}
    obj_count = minio_client.get_folder_image_last(basket, images[0]['item_id'])
    obj_count = 0 if obj_count < 0 else obj_count
    counter = 1 if obj_count == 0 else obj_count
    for item in images:
        item_id = item['item_id']
        try:
            is_main = item['is_main']
        except KeyError:
            is_main = 0
        photo_blob = item['base64']
        extension = None
        image_data = None
        try:
            image_data = base64.b64decode(photo_blob)
            extension = imghdr.what(None, h=image_data)
        except Exception as err:
            print(err)
        if not extension and photo_blob.index('base64') > -1:
            extension = photo_blob.split(';', 1)[0].split('/', 1)[-1]
            image_data = base64.b64decode(photo_blob.split(',', 1)[1])
        if is_main == 0:
            file_name = f'item_{counter}.{extension}'
            counter += 1
        else:
            file_name = f'item_{0}.{extension}'
        if extension is None:
            return {'status': -1, 'err_msg': 'Unable to determine file extension.'}
        try:
            image = Image.open(BytesIO(image_data))
            image_data = compress_image(image, size)
            minio_client.upload_file(bucket_name=basket, object_name=f'{item_id}/' + file_name,
                                     file_path=BytesIO(image_data), file_len=len(image_data))
        except Exception as err:
            return {"status": -1, 'err_msg': format(err)}
    return {'status': 0, 'err_msg': ''}


def edit_images(editImage: models.EditImage):
    url = editImage.url
    operation_type = editImage.operation_type
    basket = editImage.basket
    objects = list(filter(bool, parser.urlparse(url).path.split('/')))
    if operation_type == 'update':
        file = editImage.replace_file
        image_data = base64.b64decode(file)
        extension = imghdr.what(None, h=image_data)
        if extension is None:
            return {'status': -1, 'err_msg': 'Unable to determine file extension.'}
        try:
            minio_client.upload_file(bucket_name=basket, object_name=f'{objects[-2]}/' + objects[-1],
                                     file_path=BytesIO(image_data), file_len=len(image_data))
        except Exception as err:
            return {"status": -1, 'err_msg': format(err)}
        else:
            return {"status": 0, 'err_msg': "Image successfully updated"}
    elif operation_type == 'delete':
        try:
            minio_client.delete_file(basket, objects[-2], objects[-1])
        except Exception as err:
            return {"status": -1, 'err_msg': format(err)}
        else:
            return {"status": 0, 'err_msg': "Image successfully deleted"}
    else:
        return {'status': -1, "err_msg": "Invalid operation"}
