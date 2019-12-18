import json
import time
import redis


class MeterModel:
    def __init__(self, res_id, tag):
        self.res_id = res_id
        self.tag = res_id + "_" + tag
        self.need_update = False
        self.data = dict()

    def __getattr__(self, item):
        """
        将字段作为对象属性调用
        """
        if item in self.data:
            val = self.data[item]
            return val

    def set(self, key, value):
        if key in self.data:
            if self.data[key] != value:
                self.data[key] = value
                self.need_update = True
        else:
            return None

    @staticmethod
    def instance(res_id, redis_client, tag):
        """
        从redis中实例化对象
        """
        # 从redis中获取值
        meter = MeterModel(res_id, tag)
        if redis_client.exists(meter.tag):
            val = redis_client.get(meter.tag)
            meter.data = json.loads(val)
            return meter
        else:
            # 发送再次查询消息
            redis_client.rpush("db_get", meter.tag)
            for times in range(3):
                time.sleep(0.2)
                if redis_client.exists(meter.tag):
                    val = redis_client.get(meter.tag)
                    meter.data = json.loads(val)
                    return meter

    def update(self, redis_client):
        """
        更新相关数据，并向数据服务发送更新消息，提醒服务将数据保存到数据库
        """
        if self.need_update:
            redis_client.set(self.tag, json.dumps(self.data))
            redis_client.rpush("db_update", self.tag)
            self.need_update = False


if __name__ == '__main__':
    """
    测试代码
    """
    client = redis.Redis("10.27.0.8")
    m = MeterModel.instance("1110119999910", client, "key")
    if m:
        print(m.ResKey)
        print(m.ResType)
    else:
        print("Not Exists")