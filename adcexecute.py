import threading
from concurrent import futures
from concurrent.futures import wait
from sogou_apihandle import ApiHandle
from readmysqldata import Write
from sogouUtils import Utils
from handleconn import Conn
from loggers.logger import Logger

"""初始化线程池"""
ex = futures.ThreadPoolExecutor(max_workers=15)
"""初始化redis"""
redis = Conn().redis_pool
"""初始化代理商信息"""
adgAdvId = ApiHandle().getAdgId()


class Adc:

    def __init__(self):
        """获取全部的推广组ID"""
        # self.allAdgId = ApiHandle().getAdgId()
        self.service_name_adc = 'CpcIdeaService'
        self.interface_name_adc = 'getCpcIdeaByCpcGrpId'
        self.adcId = 'cpcIdeaId'
        self.grpId = 'cpcGrpIds'
        self.adcHbase = 'sogou_creatives'
        """初始化一个锁"""
        self.lock = threading.Lock()
        self.yesterday = Utils().yeaterday
        self.adcHbase = 'sogou_creatives'
        self.adcnull = 0
        self.adcnumber = 0
        self.redisKey = 'sogou_adcId_'
        self.listEmpty = []

    def getAdcData(self, adgId):
        # adgId--->"""代理商ID，代理商名称，代理商密码,广告主名称,广告主Id,广告计划Id,广告组Id,token"""
        tag = ''
        for adgIdList in adgId[4]:
            tag += '<cpcGrpIds> {} </cpcGrpIds>'.format(adgIdList)
        bodyAdg = """
           <soapenv:Body>
                <v11:{}>
                {}
                </v11:{}>
           </soapenv:Body>
           """.format(self.interface_name_adc, tag, self.interface_name_adc)
        xml = ApiHandle().sendHttpRequest(self.service_name_adc, adgId[0], adgId[1], adgId[2], adgId[5], bodyAdg)
        xmlDict = Utils().xMLParser(xml)
        if self.adcId in xmlDict.keys():
            """将cpcTypes标签里的数据取出来"""
            camIdTagsList = Utils().xmlParserList(xml, '<cpcIdeaTypes>', '</cpcIdeaTypes>')
            """将每一个标签内容解析为dict格式"""
            camDictList = Utils().xmlParserAll(camIdTagsList)
            redis.incr(self.interface_name_adc + self.yesterday, len(camDictList))
            self.lock.acquire()
            try:
                self.adcnumber += len(camDictList)
            finally:
                self.lock.release()
            for camDict in camDictList:
                del camDict['negativeWords']
                del camDict['exactNegativeWords']
                mergeData = Utils().matchAdcChinese(camDict)
                """将广告组Id保存到redis里--->sogou_adcId_20180429,广告主id,广告组Id+关键词Id"""
                # self.putIdToRedis(mergeData['cpcGrpId'], mergeData[self.adcId], adgId[3])
                redis.zadd(self.redisKey + self.yesterday, '{}_{}'.format(mergeData['cpcGrpId'], mergeData[self.adcId]),
                           adgId[3])
                redis.rpush('sogou_adcId_prepare_' + self.yesterday,
                            '{}_{}'.format(mergeData['cpcGrpId'], mergeData[self.adcId]))
                redis.expire(self.redisKey + self.yesterday, 30 * 24 * 3600)
                """将广告主信息保存到hbase里--->0|20180428+accountid+adgid+keyid"""
                self.putAdcDataToHbase(adgId[3], mergeData['cpcGrpId'], mergeData[self.adcId], mergeData)
                Logger().inilg(self.interface_name_adc,
                               '{} update  is success {}'.format(adgId[3], mergeData[self.adcId]))
            redis.sadd('updataadcId_vaild_accountId_' + self.yesterday, adgId[3])
            redis.expire('updataadcId_vaild_accountId_' + self.yesterday, 30 * 24 * 3600)
        else:
            self.lock.acquire()
            try:
                self.adcnull += 1
            finally:
                self.lock.release()
            Write().recordErrorLog(self.yesterday, adgId[2], self.interface_name_adc, 10000, 'Data id null!')
            Logger().inilg('{}Null'.format(self.interface_name_adc),
                           '{} update adgId is success but data is null {}'.format(adgId[2], xmlDict))


    def putAdcDataToHbase(self, *hbaseKey):
        self.lock.acquire()
        try:
            self.listEmpty.append((str(hbaseKey[0])[-1] + '|' + self.yesterday + '|' + str(hbaseKey[0]) + '|' + str(
                hbaseKey[1]) + '|' + str(hbaseKey[2]), hbaseKey[3]))
            if len(self.listEmpty) >= 5000:
                Conn().putDataToHbase(self.listEmpty, self.adcHbase)
        finally:
            self.lock.release()

    def executeAdcFunction(self):
        # 记录开始日志到MySQL里
        # adgId--->"""代理商名称，代理商密码,广告主名称,广告主Id,广告组Id集合,token"""
        Write().start_info(self.yesterday, self.grpId + self.yesterday, self.interface_name_adc)
        if len(adgAdvId) != 0:
            futures = []
            print(len(adgAdvId))
            for adgId in adgAdvId:
                futures.append(ex.submit(self.getAdcData, adgId))
            wait(futures)
            Conn().putDataToHbase(self.listEmpty, self.adcHbase)
            Logger().inilg('{}Null'.format(self.interface_name_adc),
                           'keyId data is null number is {}'.format(self.adcnull))
            Logger().inilg(self.interface_name_adc, 'adgData send hbase number is {}'.format(self.adcnumber))
        else:
            Logger().inilg(self.interface_name_adc, 'adgInformation is null,please check camId whether update!')


if __name__ == '__main__':

    """执行创意程序"""
    Adc().executeAdcFunction()

    # 记录请求次数到MySQL里
    requestTime = redis.get(Adc().service_name_adc + Adc().yesterday)
    Write().putApiStatisticsInfo(Adc().interface_name_adc, requestTime.decode())
    # 记录结束日志到MySQL里
    total = redis.get(Adc().interface_name_adc + Adc().yesterday)
    Write().end_info(-1, total.decode(), Adc().yesterday, Adc().adcId + Adc().yesterday, Adc().interface_name_adc)

    Logger().inilg(Adc().interface_name_adc, '----------------------------adcId update complete')
