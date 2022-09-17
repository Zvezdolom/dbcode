import os
import sqlite3 as sql
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import cfg


class database:
    def __init__(self, db_type: str = None, sql_init: bool = False) -> None:
        self.db_type: str = cfg.db_type if db_type is None else db_type
        self.db_id: str = cfg.db_param_id
        self.params: dict = cfg.db_schema
        match self.db_type:
            case 'sqlite3':
                self.connection = sql.connect(os.path.join(cfg.module_dir, f'{cfg.db_sql3_path}'))
                self.connection.row_factory = sql.Row
                self.cursor = self.connection.cursor()
            case 'postgresql':
                host = cfg.db_psql_host
                port = cfg.db_psql_port
                user = cfg.db_psql_user
                password = cfg.db_psql_password
                dbname = cfg.db_psql_dbname
                self.connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=dbname)
                self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            case _:
                print('[error] (__init__) database_type does not match existing')
        self.sql_init() if sql_init is True else None

    def sql_init(self) -> None:
        match self.db_type:
            case 'sqlite3':
                with open(os.path.join(cfg.module_dir, f'{cfg.db_sql3_init}'), mode='r', encoding=f'{cfg.db_encode}') as file:
                    self.cursor.executescript(file.read())
                self.connection.commit()
            case 'postgresql':
                with open(os.path.join(cfg.module_dir, f'{cfg.db_psql_init}'), mode='r', encoding=f'{cfg.db_encode}') as file:
                    self.cursor.execute(file.read())
                self.connection.commit()
            case _:
                print('[error] (create) database_type does not match existing')

    def raw(self, request: str) -> list or bool or None:
        try:
            request: str = f"{request}"
            self.cursor.execute(request)
            result = self.cursor.fetchall()
            if not result:
                return None
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (raw) ' + str(e))
        return False

    def select(self, table: str, fields: str or int or list = None, limit: int = None, cut: bool = False) -> list or bool or None:
        fields = [fields] if isinstance(fields, int) or isinstance(fields, str) else fields
        field: str = ', '.join(str(fields[i]) for i in range(0, len(fields))) if fields is not None else '*'
        limit: str = '' if limit is None else f' LIMIT {limit}'
        try:
            request: str = f"SELECT {field} FROM {table}{limit}"
            self.cursor.execute(request)
            result = self.cursor.fetchall()
            if not result:
                return None
            if len(result) == 1 and cut is True:
                result = result[0]
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select) ' + str(e))
        return False

    def select_where(self, table: str, params: str or int or list, values: str or int or list, fields: str or int or list = None, limit: int = None, cut: bool = False) -> list or bool or None:
        params = [params] if isinstance(params, int) or isinstance(params, str) else params
        values = [values] if isinstance(values, int) or isinstance(values, str) else values
        fields = [fields] if isinstance(fields, int) or isinstance(fields, str) else fields
        field: str = ', '.join(str(fields[i]) for i in range(0, len(fields))) if fields is not None else '*'
        p_v: str = ' AND '.join(f"{params[i]}='{values[i]}'" for i in range(0, len(params)))
        limit = '' if limit is None else f' LIMIT {limit}'
        try:
            request: str = f"SELECT {field} FROM {table} WHERE {p_v}{limit}"
            self.cursor.execute(request)
            result = self.cursor.fetchall()
            if not result:
                return None
            if len(result) == 1 and cut is True:
                result = result[0]
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_where) ' + str(e))
        return False

    def select_distinct(self, table: str, param: str, limit: int = None, cut: bool = False) -> list or bool or None:
        limit: str = '' if limit is None else f' LIMIT {limit}'
        try:
            request: str = f"SELECT DISTINCT {param} FROM {table}{limit}"
            self.cursor.execute(request)
            result = self.cursor.fetchall()
            if not result:
                return None
            if len(result) == 1 and cut is True:
                result = result[0]
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_distinct) ' + str(e))
        return False

    def select_count(self, table: str) -> int or bool or None:
        try:
            request: str = f"SELECT count(*) as count FROM {table}"
            self.cursor.execute(request)
            result = int(self.cursor.fetchone()['count'])
            if not result:
                return None
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_count) ' + str(e))
        return False

    def select_count_where(self, table: str, params: str or int or list, values: str or int or list) -> int or bool or None:
        params = [params] if isinstance(params, int) or isinstance(params, str) else params
        values = [values] if isinstance(values, int) or isinstance(values, str) else values
        p_v: str = ' AND '.join(f"{params[i]}='{values[i]}'" for i in range(0, len(params)))
        try:
            request: str = f"SELECT count(*) as count FROM {table} WHERE {p_v}"
            self.cursor.execute(request)
            result = int(self.cursor.fetchone()['count'])
            if not result:
                return None
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_count_where) ' + str(e))
        return False

    def update(self, table: str, params1: str or int or list, values1: str or int or list, params2: str or int or list, values2: str or int or list) -> bool:
        params1 = [params1] if isinstance(params1, int) or isinstance(params1, str) else params1
        values1 = [values1] if isinstance(values1, int) or isinstance(values1, str) else values1
        params2 = [params2] if isinstance(params2, int) or isinstance(params2, str) else params2
        values2 = [values2] if isinstance(values2, int) or isinstance(values2, str) else values2
        p_v1: str = ', '.join(f"{params1[i]}='{values1[i]}'" for i in range(0, len(params1)))
        p_v2: str = ' AND '.join(f"{params2[i]}='{values2[i]}'" for i in range(0, len(params2)))
        try:
            request: str = f"UPDATE {table} SET {p_v1} WHERE {p_v2}"
            self.cursor.execute(request)
            self.connection.commit()
            return True
        except psycopg2.Error or sql.Error as e:
            print('[error] (update) ' + str(e))
        return False

    def insert(self, table: str, values: str or int or list, params: str or int or list = None, _id: str = None) -> int or bool:
        values = [values] if isinstance(values, int) or isinstance(values, str) else values
        params = [params] if isinstance(params, int) or isinstance(params, str) else params
        param: str = ', '.join(str(i) for i in params) if params is not None else ', '.join(self.params[f'{table}'])
        value: str = "', '".join([str(i) for i in values])
        if param == '' and value == '':
            param: str = ', '.join(self.params[f'{table}'])
            value: str = "', '".join(['' for _ in range(0, len(self.params[f'{table}']), 1)])
        if _id is not None:
            param, value = f"{self.db_id}, {param}", f"{_id}', '{value}"
        last_id: int = 0
        try:
            match self.db_type:
                case 'sqlite3':
                    request: str = f"INSERT INTO {table} ({param}) VALUES ('{value}')"
                    self.cursor.execute(request)
                    last_id: int = self.cursor.lastrowid
                case 'postgresql':
                    request: str = f"INSERT INTO {table} ({param}) VALUES ('{value}') RETURNING {self.db_id}"
                    self.cursor.execute(request)
                    result = self.cursor.fetchone()[self.db_id]
                    last_id: int = int(result)
            self.connection.commit()
            return last_id
        except psycopg2.Error or sql.Error as e:
            print('[error] (insert) ' + str(e))
        return False

    def delete(self, table: str, params: str or int or list, values: str or int or list) -> bool:
        values = [values] if isinstance(values, int) or isinstance(values, str) else values
        params = [params] if isinstance(params, int) or isinstance(params, str) else params
        p_v: str = ' AND '.join(f"{params[i]}='{values[i]}'" for i in range(0, len(params)))
        try:
            request: str = f"DELETE FROM {table} WHERE {p_v}"
            self.cursor.execute(request)
            self.connection.commit()
            return True
        except psycopg2.Error or sql.Error as e:
            print('[error] (delete) ' + str(e))
        return False

    def create_table(self, table: str, params: list = None) -> bool:
        cfg.add_table(table, params) if params is not None else None
        try:
            params: str = self.params[table] if params is None else params
            params: str = ', '.join(f'{i} TEXT' for i in params)
            match self.db_type:
                case 'sqlite3':
                    params = f'{self.db_id} INTEGER NOT NULL UNIQUE, {params}, PRIMARY KEY ({self.db_id} AUTOINCREMENT)'
                case 'postgresql':
                    params = f'{self.db_id} INT PRIMARY KEY NOT NULL, {params}'
                case _:
                    print('[error] (create_table) database_type does not match existing')
            request: str = f"CREATE TABLE IF NOT EXISTS {table} ({params});"
            self.cursor.execute(request)
            if self.db_type == 'postgresql':
                request: str = f"CREATE SEQUENCE IF NOT EXISTS {table}_seq INCREMENT 1 START 1 NO CYCLE OWNED BY {table}.{self.db_id};"
                self.cursor.execute(request)
                request: str = f"ALTER TABLE {table} ALTER COLUMN {self.db_id} SET DEFAULT nextval('{table}_seq');"
                self.cursor.execute(request)
            self.connection.commit()
            return True
        except psycopg2.Error or sql.Error as e:
            print('[error] (create_table) ' + str(e))
        return False

    def create_base(self) -> None:
        tables: list = list(self.params.keys())
        for table in tables:
            self.create_table(table)

    def drop_table(self, table: str) -> bool:
        cfg.del_table(table)
        try:
            request: str = f"DROP TABLE {table}"
            self.cursor.execute(request)
            self.connection.commit()
            return True
        except psycopg2.Error or sql.Error as e:
            print('[error] (drop_table) ' + str(e))
        return False

    def drop_base(self) -> None:
        tables: list = list(self.params.keys())
        for table in tables:
            self.drop_table(table)


db = database()