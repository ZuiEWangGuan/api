import threading
from concurrent import futures
from concurrent.futures import wait
from sogou_apihandle import ApiHandle
from readmysqldata import Write
from sogouUtils import Utils
from handleconn import Conn
from loggers.logger import Logger

"""初始化线程池"""
ex = futures.ThreadPoolExecutor(max_workers=10)
"""初始化redis"""
redis = Conn().redis_pool
"""初始化代理商信息"""
camAgsId = ApiHandle().getCamId()


class Adg:

    def __init__(self):
        self.service_name_adg = 'CpcGrpService'
        self.interface_name_adg = 'getCpcGrpByCpcPlanId'
        self.planId = 'cpcPlanIds'
        self.adgId = 'cpcGrpId'
        self.adgHbase = 'sogou_adgroups'
        self.yesterday = Utils().yeaterday
        """初始化一个锁"""
        self.lock = threading.Lock()
        self.adgnumber = 0
        self.adgnull = 0
        self.listEmpty = []
        self.redisKey = 'sogou_adgId_'
        self.getTemp='<getTemp>0</getTemp>'

    def getAdgData(self, camId):
        tag = ''
        for camIdList in camId[4]:
            tag += '<cpcPlanIds> {} </cpcPlanIds>'.format(camIdList)
        bodyCam = """
            <soapenv:Body>
                 <v11:{}>
                   {}
                 </v11:{}>
            </soapenv:Body>
            """.format(self.interface_name_adg, tag,self.interface_name_adg)
        # camId--->"""代理商名称，代理商密码,广告主名称,广告主Id,广告计划Id集合,token"""
        """"传入的参数：服务名，代理商名，代理商密码，用户名，token，httpbody"""
        xml = ApiHandle().sendHttpRequest(self.service_name_adg, camId[0],
                                          camId[1], camId[2], camId[5], bodyCam)
        xmlDict =Utils().xMLParser(xml)
        if self.adgId in xmlDict.keys():
            """将cpcPlanTypes标签里的数据取出来"""
            camIdTagsList = Utils().xmlParserList(xml, '<cpcGrpTypes>', '</cpcGrpTypes>')
            """将每一个标签内容解析为dict格式"""
            camDictList = Utils().xmlParserAll(camIdTagsList)
            redis.incr(self.interface_name_adg + self.yesterday, len(camDictList))
            self.lock.acquire()
            try:
                self.adgnumber += len(camDictList)
            finally:
                self.lock.release()
            for camDict in camDictList:
                mergeData = Utils().matchAdgChinese(camDict)
                """将广告组Id保存到redis里--->sogou_adgId_20180429,广告主id，广告计划Id+广告组Id"""
                redis.zadd(self.redisKey + self.yesterday, '{}_{}'.format(mergeData['cpcPlanId'], mergeData[self.adgId]), camId[3])
                redis.zadd('sogou_adgId_test_' + self.yesterday, mergeData[self.adgId], camId[3])
                redis.rpush('sogou_adgId_prepare_' + self.yesterday,'{}_{}'.format(mergeData['cpcPlanId'], mergeData[self.adgId]))
                redis.expire(self.redisKey + self.yesterday,30*24*3600)
                redis.expire('sogou_adgId_prepare_' + self.yesterday, 30 * 24 * 3600)
                #self.putIdToRedis(mergeData['cpcPlanId'], mergeData[self.adgId], camId[3])
                """将广告主信息保存到hbase里--->0|20180428+accountid+camid+adgid"""
                self.putAdgDataToHbase(camId[3],mergeData['cpcPlanId'],mergeData[self.adgId], mergeData)
                #mergeData['cpcPlanId'],
                Logger().inilg(self.interface_name_adg,
                               '{}_{} update adg is success {}'.format(camId[3],mergeData['cpcPlanId'],mergeData[self.adgId]))
            redis.sadd('updataadgId_vaild_accountId_' + self.yesterday, camId[3])
            redis.expire('updataadgId_vaild_accountId_' + self.yesterday,30*24*3600)
        else:
            self.lock.acquire()
            try:
                self.adgnull += 1
            finally:
                self.lock.release()
            Write().recordErrorLog(self.yesterday, camId[2], self.interface_name_adg, 10000, 'Data id null!')
            Logger().inilg('{}Null'.format(self.interface_name_adg),
                           '{} update adgId is success but data is null {}'.format(camId[2], xml))


    def putAdgDataToHbase(self, *hbaseKey):
        self.lock.acquire()
        try:
            self.listEmpty.append((str(hbaseKey[0])[-1] + '|' + self.yesterday + '|' + str(hbaseKey[0]) + '|' + str(hbaseKey[1]) + '|' + str(hbaseKey[2]), hbaseKey[3]))
            #+ '|' + str(hbaseKey[2])
            if len(self.listEmpty) >= 5000:
                Conn().putDataToHbase(self.listEmpty, self.adgHbase)
                del self.listEmpty[:]
        finally:
            self.lock.release()


    def executeAdgFunction(self):
        # 记录开始日志到MySQL里
        #Write().start_info(self.yesterday, self.planId + self.yesterday, self.interface_name_adg)
        # camId--->"""代理商ID，代理商名称，代理商密码,广告主名称,广告主Id,广告计划Id,token"""
        if len(camAgsId) != 0:
            futures = []
            for camId in camAgsId:
                futures.append(ex.submit(self.getAdgData, camId))
            wait(futures)
            Conn().putDataToHbase(self.listEmpty, self.adgHbase)
            Logger().inilg('{}Null'.format(self.interface_name_adg),
                           'adgId data is null number is {}'.format(self.adgnull))
            Logger().inilg(self.interface_name_adg, 'adgData send hbase number is {}'.format(self.adgnumber))
        else:
            Logger().inilg(self.interface_name_adg, 'adgInformation is null,please check camId whether update!')


if __name__ == '__main__':

    Adg().executeAdgFunction()
    # 记录请求次数到MySQL里
    requestTime = redis.get(Adg().service_name_adg + Adg().yesterday)
    Write().putApiStatisticsInfo(Adg().interface_name_adg, requestTime.decode())
    # 记录结束日志到MySQL里
    total = redis.get(Adg().interface_name_adg + Adg().yesterday)
    Write().end_info(-1, total.decode(), Adg().yesterday, Adg().adgId + Adg().yesterday, Adg().interface_name_adg)
    Logger().inilg(Adg().interface_name_adg, '----------------------------adgId update complete')
