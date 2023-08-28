from datetime import datetime, time


def time_str_to_date_picker(time_str):
    if not isinstance(time_str, time):
        time_obj = datetime.strptime(time_str, "%H:%M:%S").time()
    else:
        time_obj = time_str
    return str(time_obj.hour if time_obj.hour >= 10 else "0" + str(time_obj.hour)) + ":" + \
           str(time_obj.minute if time_obj.minute >= 10 else "0" + str(time_obj.minute))
