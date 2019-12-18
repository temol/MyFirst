"""
从redis中取出数据
"""
import redis
import time
from models import MeterModel


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='redis', max_connections=5)
    client = redis.Redis(connection_pool=pool)

    res_list = [
        '1110129000001',
        '1110129000002',
        '1110129000003',
        '1110129000004',
        '1110129000005',
        '1110129000006'
    ]
    for res in res_list:
        m = MeterModel.instance(res, client, "key")
        if m:
            print(m.ResKey, m.InputTime, m.ResType)
        else:
            print(res, "no data.")

    for res in res_list:
        m = MeterModel.instance(res, client, "set")
        if m:
            print(m.meterPrice, m.serverIP, m.serverPort)
        else:
            print(res, "no data.")

    while True:
        time.sleep(30)
