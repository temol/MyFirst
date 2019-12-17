"""
向resid中间push数据，key使用 message
"""
import redis
import time
import datetime


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='redis', max_connections=5)
    client = redis.Redis(connection_pool=pool)
    while True:
        time.sleep(1)
        s = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        client.rpush('message', s)
        print('push %s into [message]' % (s,))
