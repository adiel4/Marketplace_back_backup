import requests
import config as cfg


def get_address(coordinates: list):
    url = f"https://geocode-maps.yandex.ru/1.x/?apikey={cfg.yandex_api_key}&format=json&geocode={coordinates[1]},{coordinates[0]}"
    response = requests.get(url)
    if response.status_code == 200:
        response = response.json()
        return response.get('response').get('GeoObjectCollection').get('featureMember')[0].get('GeoObject')
    else:
        return {'status': -1, 'code': response.status_code}
