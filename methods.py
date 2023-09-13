from datetime import datetime, time
from math import sqrt, radians, cos, sin, atan2


def time_str_to_date_picker(time_str):
    if not isinstance(time_str, time):
        time_obj = datetime.strptime(time_str, "%H:%M:%S").time()
    else:
        time_obj = time_str
    return str(time_obj.hour if time_obj.hour >= 10 else "0" + str(time_obj.hour)) + ":" + \
        str(time_obj.minute if time_obj.minute >= 10 else "0" + str(time_obj.minute))


def calculating_distance(current_coordinates, point):
    # ((42.871718, 74.573392), (42.871722, 74.571203))
    # Результат в метрах
    rad = 6372795
    if current_coordinates is None and point is None:
        return None
    try:
        x_now, y_now = current_coordinates
        x_now_rad = radians(float(x_now))
        y_now_rad = radians(float(y_now))

        x_point, y_point = point
        x_point_rad = radians(float(x_point))
        y_point_rad = radians(float(y_point))

        cos_x_now = cos(x_now_rad)
        cos_x_point = cos(x_point_rad)
        sin_x_now = sin(x_now_rad)
        sin_x_point = sin(x_point_rad)
        delta = y_point_rad - y_now_rad
        cos_delta = cos(delta)

        y = sqrt((cos_x_point * sin(delta)) ** 2 +
                 (cos_x_now * sin_x_point - sin_x_now * cos_x_point * cos_delta) ** 2)
        x = sin_x_now * sin_x_point + cos_x_now * cos_x_point * cos_delta
        ad = atan2(y, x)
        dist = ad * rad
        return int(dist)

    except Exception as err:
        return f"Error on point {point}: {err}"
