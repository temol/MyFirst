"""
从redis中取出数据
"""
import redis
import time
from models import Meter
import pymssql


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='redis', max_connections=5)
    client = redis.Redis(connection_pool=pool)

    meter = Meter.instance("1110099000004", client)
    if meter:
        print(meter.ResKey, meter.FrameId, meter.OrgCode, meter.ServerIP, meter.MeterPrice,
              meter.Price0, meter.CycleStartDate)
        # meter.set_value("CallbackAddress", "")

    meter = None


