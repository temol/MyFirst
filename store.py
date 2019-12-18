"""
向resid中间push数据，key使用 message
"""
import redis
import json
import datetime
import pymssql
import decimal
import threading
import time


class DateEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(o, datetime.date):
            return o.strftime("%Y-%m-%d")
        elif isinstance(o, decimal.Decimal):
            return float(o)
        else:
            return json.JSONEncoder.default(self, o)


class DatabaseOperation:
    @staticmethod
    def get(redis_pool, db_conn):
        client = redis.Redis(connection_pool=redis_pool)
        cursor = db_conn.cursor(as_dict=True)
        while True:
            time.sleep(0.1)
            content = client.blpop("db_get", 60)
            if not content:
                continue
            content = content.decode('utf-8')
            res_id, tag = content.split('_')
            if tag == "key":
                cursor.execute("select * from tb_meter where ResID=%s", res_id)
                data = cursor.fetchone()
                if data:
                    res_info = json.dumps(data, cls=DateEncoder)
                    client.set(content, res_info)
            if tag == "set":
                cursor.execute("select * from tb_meter_set where ResId=%s", res_id)
                data = cursor.fetchone()
                if data:
                    res_info = json.dumps(data, cls=DateEncoder)
                    client.set(content, res_info)

    @staticmethod
    def update(redis_pool, db_conn):
        client = redis.Redis(connection_pool=redis_pool)
        cursor = db_conn.cursor(as_dict=True)
        while True:
            time.sleep(0.1)
            content = client.blpop("db_update", 60)
            if not content:
                continue
            content = content.decode('utf-8')
            res_id, tag = content.split('_')
            if tag == "key":
                v = client.get(content)
                if not v:
                    continue
                res_info = json.loads(v)
                sql, params = DatabaseOperation.build_sql("tb_meter", res_info, res_id)
                cursor.execute(sql, params)
                db_conn.commit()
            if tag == "set":
                v = client.get(content)
                if not v:
                    continue
                res_info = json.loads(v)
                sql, params = DatabaseOperation.build_sql("tb_meter_set", res_info, res_id)
                cursor.execute(sql, params)
                db_conn.commit()

    @staticmethod
    def build_sql(table_name, info, res_id):
        sql = "update " + table_name + " set "
        key_list = info.keys()
        val_list = list()
        for k in key_list:
            sql += k + "=%s,"
            val_list.append(info[k])
        sql = sql[0:-1] + " where ResId=%s"
        val_list.append(res_id)
        return sql, tuple(val_list)


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='redis', max_connections=5)
    # client = redis.Redis(connection_pool=pool)
    conn_get = pymssql.connect("10.27.0.2", "sa", "P@ssw0rd", "NBMeter")
    conn_update = pymssql.connect("10.27.0.2", "sa", "P@ssw0rd", "NBMeter")
    # 建立线程处理
    get_thread = threading.Thread(target=DatabaseOperation.get, args=(pool, conn_get))
    update_thread = threading.Thread(target=DatabaseOperation.update, args=(pool, conn_update))

    get_thread.start()
    update_thread.start()

    while True:
        time.sleep(10)

