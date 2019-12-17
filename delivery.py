"""
从redis中取出数据
"""
import redis
import time


if __name__ == '__main__':
    client = redis.Redis("redis")

    while True:
        time.sleep(0.1)
        x = client.blpop("message", 120)
        print('pull from [message]:  %s' % (x.decode(),))
