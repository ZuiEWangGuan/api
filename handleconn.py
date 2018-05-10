import redis
import json
import happybase
from retrying import retry
from readconfig import Read
from loggers.logger import Logger

hbase_pool = happybase.ConnectionPool(size=2, host=Read().hbase_host)

class Conn:

    def __init__(self):
        """初始化redis和hbase连接"""
        self.redis_pool=self.getRedisConn()


    @staticmethod
    @retry(stop_max_attempt_number=5, wait_fixed=0.5)
    def getRedisConn():
        """获取redis连接"""
        pool = redis.ConnectionPool(host=Read().redis_host, port=int(Read().redis_port),
                                    password=Read().redis_passwd)
        redis_state = redis.Redis(connection_pool=pool,charset='utf-8')
        return redis_state


    #异常重试
    @retry(stop_max_attempt_number=5, wait_fixed=0.5)
    def putDataToHbase(self,DataList,tableName):
        """发送数据到hbase里"""
        with hbase_pool.connection() as connection:
            table=connection.table(tableName)
            batch = table.batch()
            with batch as bac:
                for row_key,value in DataList:
                    valueString=json.dumps(value)
                    bac.put(row=row_key,data={'info:data':valueString})
        Logger().inilg(tableName, 'hbase data  send success {} number'.format(len(DataList)))
        del DataList[:]











