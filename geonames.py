import requests


def get_place_name_by_coordinates(lat, lng):
    url = "http://api.geonames.org/findNearbyPlaceNameJSON"
    params = {
        'lat': lat,
        'lng': lng,
        'username': 'dzhavmashev',
        'lang': 'ru'
    }

    try:
        response = requests.get(url, params=params)
        data = response.json()
        if 'geonames' in data and len(data['geonames']) > 0:
            return {'status': 0, 'data': data.get('geonames')[0]}
        else:
            return {'status': -1, 'err_msg': 'Не удалось найти место'}
    except Exception as err:
        return {'status': -1, 'err_msg': format(err)}

