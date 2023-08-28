import qrcode
from cryptography.fernet import Fernet
import base64
import io


def encrypt_data(data, secret_key):
    return Fernet(secret_key).encrypt(data.encode('utf-8'))


def decrypt_data(encrypted_data, secret_key):
    fernet = Fernet(secret_key)
    decrypted_data = fernet.decrypt(encrypted_data)
    return decrypted_data.decode('utf-8')


def generate_qr_code(data):
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer)
    return 'data:image/png;base64,' + base64.b64encode(buffer.getvalue()).decode("utf-8")


def save_qr(data, c_id: int):
    key = Fernet.generate_key()
    full_data = str(c_id) + encrypt_data(data, key).decode('utf-8')
    return {'qr_base64': generate_qr_code(full_data), 'key': key.decode('utf-8')}
