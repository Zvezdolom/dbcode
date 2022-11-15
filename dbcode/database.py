import os
import psycopg2
import sqlite3 as sql
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from .config import config


cfg = config()


def deprecated(fn):
    def function(*args):
        function_name = fn.__name__
        print(f'[warning] ({function_name}) this function is deprecated!')
        return fn(*args)
    return function


class database:
    def __init__(self, db_type: str = None, sql_init: bool = False) -> None:
        self.db_type: str = cfg.db_type if db_type is None else db_type
        self.db_id: str = cfg.db_param_id
        self.params: dict = cfg.db_schema
        self.debug: bool = cfg.db_debug
        match self.db_type:
            case 'sqlite3':
                self.connection = sql.connect(os.path.join(cfg.module_dir, f'{cfg.db_sql3_path}'))
                self.connection.row_factory = sql.Row
                self.cursor = self.connection.cursor()
            case 'postgresql':
                host: str = cfg.db_psql_host
                port: str = cfg.db_psql_port
                user: str = cfg.db_psql_user
                password: str = cfg.db_psql_password
                dbname: str = cfg.db_psql_dbname
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
            print(f'[debug] (raw) request: {request}') if self.debug else None
            self.cursor.execute(request)
            result = self.cursor.fetchall()
            return None if not result else result
        except psycopg2.Error or sql.Error as e:
            print('[error] (raw) ' + str(e))
        return False

    def select(self, table: str, params: str or int or list = None, values: str or int or list = None, fields: str or int or list = None, limit: int = None, cut: bool = False, distinct: bool = False, order_fields: str or int or list = None, order_type: str = None) -> list or bool or None:
        params: list = [params] if isinstance(params, int) or isinstance(params, str) else params
        values: list = [values] if isinstance(values, int) or isinstance(values, str) else values

        fields: list = [fields] if isinstance(fields, int) or isinstance(fields, str) else fields
        field: str = ', '.join(str(fields[i]) for i in range(0, len(fields))) if fields is not None else '*'

        distinct: str = 'DISTINCT' if distinct else ''

        param_not: list = ['' for _ in range(len(params))] if params or values is not None else []

        if params is not None:
            for i in range(len(params)):
                if params[i][0] == '!':
                    param_not[i] = '!'
                    params[i] = params[i][1:]

        p_v: str = 'WHERE ' + ' AND '.join(f"{params[i]}{param_not[i]}='{values[i]}'" for i in range(0, len(params))) if params is not None else ''

        limit: str or int = '' if limit is None else f'LIMIT {limit}'

        order_fields: list = [order_fields] if isinstance(order_fields, int) or isinstance(order_fields, str) else order_fields
        order_field: str = ', '.join(str(order_fields[i]) for i in range(0, len(order_fields))) if order_fields is not None else '*'
        order_type: str = order_type if order_type == 'DESC' else 'ASC'
        order: str = f'ORDER BY {order_field} {order_type}' if order_fields is not None else ''

        try:
            request: str = f"SELECT {distinct} {field} FROM {table} {p_v} {order} {limit}"
            print(f'[debug] (select_where) request: {request}') if self.debug else None
            self.cursor.execute(request)
            result = self.cursor.fetchall()
            if not result:
                return None
            if len(result) == 1 and cut is True:
                result = result[0]
                if len(fields) == 1:
                    result = result[0]
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_where) ' + str(e))
        return False

    @deprecated
    def select_where(self, table: str, params: str or int or list, values: str or int or list, fields: str or int or list = None, limit: int = None, cut: bool = False, distinct: bool = False, order_fields: str or int or list = None, order_type: str = None) -> list or bool or None:
        params: list = [params] if isinstance(params, int) or isinstance(params, str) else params
        values: list = [values] if isinstance(values, int) or isinstance(values, str) else values
        fields: list = [fields] if isinstance(fields, int) or isinstance(fields, str) else fields
        field: str = ', '.join(str(fields[i]) for i in range(0, len(fields))) if fields is not None else '*'
        distinct: str = 'DISTINCT' if distinct else ''
        param_not: list = ['' for _ in range(len(params))]
        for i in range(len(params)):
            if params[i][0] == '!':
                param_not[i] = '!'
                params[i] = params[i][1:]
        p_v: str = ' AND '.join(f"{params[i]}{param_not[i]}='{values[i]}'" for i in range(0, len(params)))
        limit: str or int = '' if limit is None else f'LIMIT {limit}'
        order_fields: list = [order_fields] if isinstance(order_fields, int) or isinstance(order_fields,str) else order_fields
        order_field: str = ', '.join(str(order_fields[i]) for i in range(0, len(order_fields))) if order_fields is not None else '*'
        order_type: str = order_type if order_type == 'DESC' else 'ASC'
        order: str = f'ORDER BY {order_field} {order_type}' if order_fields is not None else ''
        try:
            request: str = f"SELECT {distinct} {field} FROM {table} WHERE {p_v} {order} {limit}"
            print(f'[debug] (select_where) request: {request}') if self.debug else None
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

    @deprecated
    def select_distinct(self, table: str, param: str, limit: int = None, cut: bool = False) -> list or bool or None:
        limit: str or int = '' if limit is None else f'LIMIT {limit}'
        try:
            request: str = f"SELECT DISTINCT {param} FROM {table} {limit}"
            print(f'[debug] (select_distinct) request: {request}') if self.debug else None
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

    def select_count(self, table: str, params: str or int or list = None, values: str or int or list = None) -> int or bool or None:
        params: list = [params] if isinstance(params, int) or isinstance(params, str) else params
        values: list = [values] if isinstance(values, int) or isinstance(values, str) else values
        p_v: str = 'WHERE ' + ' AND '.join(f"{params[i]}='{values[i]}'" for i in range(0, len(params))) if params and values else ''
        try:
            request: str = f"SELECT count(*) as count FROM {table} {p_v}"
            print(f'[debug] (select_count) request: {request}') if self.debug else None
            self.cursor.execute(request)
            result = int(self.cursor.fetchone()['count'])
            if not result:
                return None
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_count) ' + str(e))
        return False

    @deprecated
    def select_count_where(self, table: str, params: str or int or list, values: str or int or list) -> int or bool or None:
        params: list = [params] if isinstance(params, int) or isinstance(params, str) else params
        values: list = [values] if isinstance(values, int) or isinstance(values, str) else values
        p_v: str = ' AND '.join(f"{params[i]}='{values[i]}'" for i in range(0, len(params)))
        try:
            request: str = f"SELECT count(*) as count FROM {table} WHERE {p_v}"
            print(f'[debug] (select_count_where) request: {request}') if self.debug else None
            self.cursor.execute(request)
            result = int(self.cursor.fetchone()['count'])
            if not result:
                return None
            return result
        except psycopg2.Error or sql.Error as e:
            print('[error] (select_count_where) ' + str(e))
        return False

    def update(self, table: str, params_a: str or int or list, values_a: str or int or list, params_b: str or int or list, values_b: str or int or list) -> bool:
        params_a: list = [params_a] if isinstance(params_a, int) or isinstance(params_a, str) else params_a
        values_a: list = [values_a] if isinstance(values_a, int) or isinstance(values_a, str) else values_a
        params_b: list = [params_b] if isinstance(params_b, int) or isinstance(params_b, str) else params_b
        values_b: list = [values_b] if isinstance(values_b, int) or isinstance(values_b, str) else values_b
        p_v_set: str = ', '.join(f"{params_a[i]}='{values_a[i]}'" for i in range(0, len(params_a)))
        p_v_where: str = ' AND '.join(f"{params_b[i]}='{values_b[i]}'" for i in range(0, len(params_b)))
        try:
            request: str = f"UPDATE {table} SET {p_v_set} WHERE {p_v_where}"
            print(f'[debug] (update) request: {request}') if self.debug else None
            self.cursor.execute(request)
            self.connection.commit()
            return True
        except psycopg2.Error or sql.Error as e:
            print('[error] (update) ' + str(e))
        return False

    def insert(self, table: str, values: str or int or list, params: str or int or list = None, _id: str = None) -> int or bool:
        values: list = [values] if isinstance(values, int) or isinstance(values, str) else values
        params: list = [params] if isinstance(params, int) or isinstance(params, str) else params
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
                    print(f'[debug] (insert) request: {request}') if self.debug else None
                    self.cursor.execute(request)
                    last_id: int = self.cursor.lastrowid
                case 'postgresql':
                    request: str = f"INSERT INTO {table} ({param}) VALUES ('{value}') RETURNING {self.db_id}"
                    print(f'[debug] (insert) request: {request}') if self.debug else None
                    self.cursor.execute(request)
                    result = self.cursor.fetchone()[self.db_id]
                    last_id: int = int(result)
            self.connection.commit()
            return last_id
        except psycopg2.Error or sql.Error as e:
            print('[error] (insert) ' + str(e))
        return False

    def delete(self, table: str, params: str or int or list, values: str or int or list) -> bool:
        values: list = [values] if isinstance(values, int) or isinstance(values, str) else values
        params: list = [params] if isinstance(params, int) or isinstance(params, str) else params
        p_v: str = ' AND '.join(f"{params[i]}='{values[i]}'" for i in range(0, len(params)))
        try:
            request: str = f"DELETE FROM {table} WHERE {p_v}"
            print(f'[debug] (delete) request: {request}') if self.debug else None
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
            print(f'[debug] (create_table) request: {request}') if self.debug else None
            self.cursor.execute(request)
            if self.db_type == 'postgresql':
                request: str = f"CREATE SEQUENCE IF NOT EXISTS {table}_seq INCREMENT 1 START 1 NO CYCLE OWNED BY {table}.{self.db_id};"
                print(f'[debug] (create_table) request: {request}') if self.debug else None
                self.cursor.execute(request)
                request: str = f"ALTER TABLE {table} ALTER COLUMN {self.db_id} SET DEFAULT nextval('{table}_seq');"
                print(f'[debug] (create_table) request: {request}') if self.debug else None
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
            print(f'[debug] (drop_table) request: {request}') if self.debug else None
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