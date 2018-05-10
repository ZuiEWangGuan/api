from concurrent import futures
from sogouUtils import Utils
from handleconn import Conn
from concurrent.futures import wait
from loggers.logger import Logger
"""初始化线程池"""
ex = futures.ThreadPoolExecutor(max_workers=10)
"""初始化redis"""
redis = Conn().redis_pool

class Set:

    def __init__(self):
        self.yesterday = Utils().yeaterday


    def executeFunction(self,adgId):
        list=str(adgId).split('_')
        redis.zadd('sogou_adgId_{}'.format(self.yesterday),'{}_{}'.format(list[1],list[2]),list[0])
        p=redis.zrangebyscore('sogou_adgId_{}'.format(self.yesterday),list[0],list[0])
        Logger().inilg('redis',p.decode())


    def WriteToRedis(self):
        adgAllId=redis.lrange('sogou_adgId_prepare_{}'.format(self.yesterday), 0, -1)
        k = redis.llen('sogou_adgId_prepare_20180508')
        print(len(adgAllId))
        print(k)
        futures=[]
        for adgId in adgAllId:
            futures.append(ex.submit(self.executeFunction, adgId.decode()))
        wait(futures)


if __name__ == '__main__':

    Set().WriteToRedis()











