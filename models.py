import json
import time
import redis
import datetime
import decimal


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


class Meter:
    """
    表具状态数据对象
    """
    def __init__(self, res_id, redis):
        self.res_id = res_id
        self.__data = dict()
        self.__is_change = False
        self.__redis = redis

    def __getattr__(self, item):
        """
        将字段作为对象属性调用
        """
        if item in self.__data:
            val = self.__data[item]
            return val

    def __del__(self):
        if self.__is_change:
            self.__redis.set(self.res_id, json.dumps(self.__data), xx=True)
            self.__redis.rpush("meter_update", self.res_id)

    def set_value(self, key, value):
        """
        设置字段的值
        :param key:
        :param value:
        :return:
        """
        if key in self.__data.keys():
            if self.__data[key] != value:
                self.__data[key] = value
                self.__is_change = True

    @staticmethod
    def instance(res_id, redis_client, force=False):
        """
        初始化一个Meter对象
        :param res_id:          表具编号
        :param redis_client:    redis连接对象
        :param force:           是否强制从DB中加载
        :return:                返回Meter对象，不存在则返回空
        """
        meter = Meter(res_id, redis_client)
        if force:
            redis_client.delete(res_id)
        if redis_client.exists(res_id):
            val = redis_client.get(res_id)
            meter.__data = json.loads(val)
            return meter
        else:
            # 发送查询数据库消息
            redis_client.rpush("meter_get", res_id)
            for times in range(3):
                time.sleep(0.1)
                if redis_client.exists(res_id):
                    val = redis_client.get(res_id)
                    meter.__data = json.loads(val)
                    return meter

    @staticmethod
    def load_from_db(res_id, redis_client, db_conn):
        """
        数据库同步线程中调用的方法：从SQL Server中加载数据到Redis中
        :param res_id:
        :param redis_client:
        :param db_conn:
        :return:
        """
        sql = """
            SELECT
                A.ResKey,
                A.ResType,
                A.FrameId,
                A.CallbackAddress,
                A.OrgCode,
                B.server_change AS ServerChange,
                B.ServerIP,
                B.ServerPort,
                B.price_display AS PriceDisplay,
                B.MeterPrice,
                B.key_change AS KeyChange,
                B.NewKey,
                B.valve_set AS ValveSet,
                B.charge_tag AS ChargeTag,
                B.meter_read_change AS MeterReadChange,
                B.valve_tcis AS ValveTcis,
                B.commu_type AS CommuType,
                B.comm_at_time AS CommAtTime,
                B.max_failure_count AS MaxFailureCount,
                C.Price0,
                C.Price1,
                C.Price2,
                C.Price3,
                C.Start1,
                C.Start2,
                C.Start3,
                C.CycleLength,
                C.CycleStartDate,
                C.CurrentCycleGas
            FROM
	            tb_meter A
	        LEFT JOIN tb_meter_set B ON A.resid = B.resid
	        LEFT JOIN tb_meter_price C ON A.resid = C.resid
            WHERE
	            A.resid = %s
        """
        cursor = db_conn.cursor(as_dict=True)
        cursor.execute(sql, res_id)
        data = cursor.fetchone()
        if data:
            redis_client.set(res_id, json.dumps(data, cls=DateEncoder), ex=172800)

    @staticmethod
    def save_to_db(res_id, redis_client, db_conn):
        """
        数据库同步线程中调用的方法：将Redis中的数据保存到SQL Server中
        :param res_id:
        :param redis_client:
        :param db_conn:
        :return:
        """
        val = redis_client.get(res_id)
        if not val:
            return
        meter = json.loads(val)
        cursor = db_conn.cursor()
        # 数据更新到tb_meter表
        sql = """
            UPDATE tb_meter
            SET
                ResKey = %s,
                ResType = %s,
                FrameID = %s,
                CallbackAddress = %s,
                OrgCode = %s
            WHERE
                ResID = %s
        """
        cursor.execute(sql, (
            meter["ResKey"],
            meter["ResType"],
            meter["FrameId"],
            meter["CallbackAddress"],
            meter["OrgCode"],
            res_id
        ))
        # 数据更新到tb_meter_set表
        sql = """
            UPDATE tb_meter_set
            SET
                server_change = %s,
                serverIp = %s,
                serverPort = %s,
                price_display = %s,
                meterPrice = %s,
                key_change = %s,
                newkey = %s,
                valve_set = %s,
                charge_tag = %s,
                meter_read_change = %s,
                valve_tcis = %s,
                commu_type = %s,
                comm_at_time = %s,
                max_failure_count = %s
            WHERE
                resid = %s
        """
        cursor.execute(sql, (
            meter["ServerChange"],
            meter["ServerIP"],
            meter["ServerPort"],
            meter["PriceDisplay"],
            meter["MeterPrice"],
            meter["KeyChange"],
            meter["NewKey"],
            meter["ValveSet"],
            meter["ChargeTag"],
            meter["MeterReadChange"],
            meter["ValveTcis"],
            meter["CommuType"],
            meter["CommAtTime"],
            meter["MaxFailureCount"],
            res_id
        ))
        # 数据更新到tb_meter_price表
        sql = """
            UPDATE tb_meter_price
            SET
                price0 = %s,
                price1 = %s,
                price2 = %s,
                price3 = %s,
                start1 = %s,
                start2 = %s,
                start3 = %s,
                cyclelength = %s,
                cyclestartdate = %s,
                currentcyclegas = %s
            WHERE
                resid = %s
        """
        cursor.execute(sql, (
            meter["Price0"],
            meter["Price1"],
            meter["Price2"],
            meter["Price3"],
            meter["Start1"],
            meter["Start2"],
            meter["Start3"],
            meter["CycleLength"],
            meter["CycleStartDate"],
            meter["CurrentCycleGas"],
            res_id
        ))
        db_conn.commit()
