import os
import ast
import configparser


class config:
    def __init__(self):
        self.module_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = 'config.ini'
        self.config_encode = 'utf-8'

        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(self.module_dir, self.config_path), self.config_encode)

        self.db_type: str = str(self.config['db']['type'])
        self.db_encode: str = str(self.config['db']['encode']).lower()
        self.db_param_id: str = str(self.config['db']['param_id'])
        self.db_debug: bool = self.bool(str(self.config['db']['debug']))

        self.db_sql3_path: str = str(self.config['db_sql3']['path'])
        self.db_sql3_init: str = str(self.config['db_sql3']['init'])

        self.db_psql_host: str = str(self.config['db_psql']['host'])
        self.db_psql_port: str = str(self.config['db_psql']['port'])
        self.db_psql_user: str = str(self.config['db_psql']['user'])
        self.db_psql_password: str = str(self.config['db_psql']['password'])
        self.db_psql_dbname: str = str(self.config['db_psql']['dbname'])
        self.db_psql_init: str = str(self.config['db_psql']['init'])

    def bool(self, b):
        return b.lower() in ['true', '1', 't', 'y', 'yes']
