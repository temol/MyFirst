"""

"""
import redis
import json
import datetime
import pymssql
import decimal
import threading
import time
import setting
from models import Meter
from DBUtils.PooledDB import PooledDB


class DatabaseOperation:
    @staticmethod
    def meter_get(redis_pool, conn_pool, delay):
        while True:
            try:
                time.sleep(delay)
                client = redis.Redis(connection_pool=redis_pool)
                result = client.blpop("meter_get", 60)
                if not result:
                    continue
                res_id = result[1].decode('utf-8')
                conn = conn_pool.connection()
                Meter.load_from_db(res_id, client, conn)
            except redis.exceptions.ConnectionError as err:
                print("redis connection error.", err)
            except pymssql.Error as err:
                print("mssql error.", err)
            except:
                print("other except.")

    @staticmethod
    def meter_update(redis_pool, conn_pool, delay):
        while True:
            time.sleep(delay)
            try:
                client = redis.Redis(connection_pool=redis_pool)
                result = client.blpop("meter_update", 60)
                if not result:
                    continue
                res_id = result[1].decode('utf-8')
                conn = conn_pool.connection()
                Meter.save_to_db(res_id, client, conn)
            except redis.exceptions.ConnectionError as err:
                print("redis connection error.", err)
            except pymssql.Error as err:
                print("mssql error.", err)
            except:
                print("other except.")


if __name__ == '__main__':
    # redis连接池
    redis_conn_pool = redis.ConnectionPool(
        host=setting.redis_host,
        port=setting.redis_port,
        max_connections=5
    )
    # sql连接池
    sql_conn_pool = PooledDB(
        creator=pymssql,
        mincached=2,
        maxcached=5,
        maxconnections=6,
        blocking=True,
        maxshared=3,
        host=setting.db_host,
        port=setting.db_port,
        user=setting.db_user,
        password=setting.db_password,
        database=setting.db_database
    )
    # 创建表具基本资料及配置处理线程
    get_thread = threading.Thread(
        target=DatabaseOperation.meter_get, args=(
            redis_conn_pool, sql_conn_pool, 0.1))
    update_thread = threading.Thread(
        target=DatabaseOperation.meter_update, args=(
            redis_conn_pool, sql_conn_pool, 0.1))
    get_thread.start()
    update_thread.start()

    while True:
        time.sleep(10)
