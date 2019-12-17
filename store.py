"""
向resid中间push数据，key使用 message
"""
import redis
import time
import datetime


if __name__ == '__main__':
    client = redis.Redis("redis")
    while True:
        time.sleep(1)
        s = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        client.rpush('message', s)
        print('push %s into [message]' % (s,))
