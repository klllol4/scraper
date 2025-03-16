import traceback
from typing import Union

import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection


class DatabaseHandler:
    @staticmethod
    def get_connection(host: str, user: str, pw: str, db=None) -> Union[PooledMySQLConnection, MySQLConnectionAbstract]:
        if db:
            return mysql.connector.connect(host=host, user=user, password=pw, database=db)
        else:
            return mysql.connector.connect(host=host, user=user, password=pw)

    @staticmethod
    def check_and_create_database(db: str, host="", user="", pw="", conn=None, disconnect=True) -> None:
        cursor = None
        try:
            if not conn:
                conn = mysql.connector.connect(host=host, user=user, password=pw)
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES;")
            dbs = [row[0] for row in cursor.fetchall()]
            if db not in dbs:
                cursor.execute("CREATE DATABASE {db};".format(db=db))

        except Exception as e:
            print(e)
            print(traceback.format_exc())

        finally:
            if cursor is not None:
                cursor.close()
            if disconnect and conn is not None:
                conn.close()

    @staticmethod
    def check_and_create_table(table: str, columns: str, host="", user="", pw="", db="", conn=None, disconnect=True) -> None:
        cursor = None
        try:
            if not conn:
                conn = mysql.connector.connect(host=host, user=user, password=pw, database=db)
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]
            if table not in tables:
                cursor.execute("CREATE TABLE `{table}` ({columns});".format(table=table, columns=columns))

        except Exception as e:
            print(e)
            print(traceback.format_exc())

        finally:
            if cursor is not None:
                cursor.close()
            if disconnect and conn is not None:
                conn.close()

    @staticmethod
    def insert(template: str, data: list, host="", user="", pw="", db="", conn=None, disconnect=True) -> None:
        cursor = None
        try:
            if not conn:
                conn = mysql.connector.connect(host=host, user=user, password=pw, database=db)
            cursor = conn.cursor()
            cursor.executemany(template, data)
            conn.commit()

        except Exception as e:
            print(e)
            print(traceback.format_exc())

        if cursor is not None:
            cursor.close()
        if disconnect and conn is not None:
            conn.close()
