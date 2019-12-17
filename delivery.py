"""
从redis中取出数据
"""
import redis
import time


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='redis', max_connections=5)
    client = redis.Redis(connection_pool=pool)

    print('pop')
    while True:
        time.sleep(0.1)

        x = client.blpop("message", 120)
        print('pop from [message]:  %s' % (x[1].decode(),))
