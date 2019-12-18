"""
向resid中间push数据，key使用 message
"""
import redis
import json
import datetime
import pymssql
import decimal
import threading


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
        pass

    @staticmethod
    def update(redis_pool, db_conn):
        pass


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='10.27.0.8', max_connections=5)
    client = redis.Redis(connection_pool=pool)

    # 建立线程处理
    get_thread = threading.Thread(target=DatabaseOperation.get, args=(pool,))
    update_thread = threading.Thread(target=DatabaseOperation.update, args=(pool,))

    get_thread.start()
    update_thread.start()

    a = dict()
    with pymssql.connect('10.27.0.2', 'sa', 'P@ssw0rd', 'NBMeter') as conn:
        cursor = conn.cursor(as_dict=True)
        cursor.execute("select * from tb_meter_set")
        data = cursor.fetchall()
        if data:
            for row in data:
                res_id = row.pop("ResId") + "_info"
                res_info = json.dumps(row, cls=DateEncoder)
                # if not client.exists(res_id):
                client.set(res_id, res_info)
                print("get tb_meter_set", res_id, res_info)

        cursor.execute("select * from tb_meter")
        data = cursor.fetchall()
        if data:
            for row in data:
                res_id = row.pop("ResID") + "_key"
                res_key = json.dumps(row, cls=DateEncoder)
                client.set(res_id, res_key)
                print("get tb_meter:", res_id, res_key)
