import fdb as db_driver

import config as cfg


class Database:
    def __init__(self):
        try:
            self.con = db_driver.connect(host=cfg.host, port=cfg.port, database=cfg.db_name, user=cfg.username,
                                         password=cfg.password, role=cfg.role, charset=cfg.charset)
        except Exception as err:
            self.con = None
            print(format(err))


class FSDatabase:
    def __init__(self):
        try:
            self.con = db_driver.connect(host=cfg.fs_host, port=cfg.fs_port, database=cfg.fs_db_name, user=cfg.fs_username,
                                         password=cfg.fs_password, role=cfg.fs_role, charset=cfg.fs_charset)
        except Exception as err:
            self.con = None
            print(format(err))
