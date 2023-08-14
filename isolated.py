import fs_database_methods as fs_dm
import database_methods as db


def to_cache():
    regions = get_all_regions()


def get_all_regions():
    return fs_dm.get_regions(1000000)
