import threading
from concurrent import futures
from concurrent.futures import wait
from sogou_apihandle import ApiHandle
from readmysqldata import Write
from sogouUtils import Utils
from handleconn import Conn
from loggers.logger import Logger

"""初始化线程池"""
ex = futures.ThreadPoolExecutor(max_workers=20)
"""初始化redis"""
redis = Conn().redis_pool
"""初始化代理商信息"""
adgAdvId = ApiHandle().getAdgId()


class Key:

    def __init__(self):
        """获取全部的推广组ID"""
        # self.allAdgId = ApiHandle().getAdgId()
        self.service_name_key = 'CpcService'
        self.interface_name_key = 'getCpcByCpcGrpId'
        self.keyId = 'cpcId'
        self.grpId = 'cpcGrpIds'
        self.keyHbase = 'sogou_keywords'
        self.yesterday = Utils().yeaterday
        """初始化一个锁"""
        self.lock = threading.Lock()
        self.keynull = 0
        self.keynumber = 0
        self.redisKey = 'sogou_keyId_'
        self.listEmpty = []

    def getkeyData(self, adgId):
        # adgId---> """代理商名称，代理商密码,广告主名称,广告主Id,广告组Id集合,token"""
        tag = ''
        for adgIdList in adgId[4]:
            tag += '<cpcGrpIds> {} </cpcGrpIds>'.format(adgIdList)
        bodyAdg = """
          <soapenv:Body>
               <v11:{}>
               {}
               </v11:{}>
          </soapenv:Body>
          """.format(self.interface_name_key, tag, self.interface_name_key)
        """"传入的参数：服务名，代理商名，代理商密码，用户名，token，httpbody"""
        xml = ApiHandle().sendHttpRequest(self.service_name_key, adgId[0], adgId[1], adgId[2], adgId[5], bodyAdg)
        xmlDict = Utils().xMLParser(xml)
        if self.keyId in xmlDict.keys():
            account = {'account_id': str(adgId[3])}
            """将cpcTypes标签里的数据取出来"""
            camIdTagsList =  Utils().xmlParserList(xml, '<cpcTypes>', '</cpcTypes>')
            """将每一个标签内容解析为dict格式"""
            camDictList =  Utils().xmlParserAll(camIdTagsList)
            redis.incr(self.interface_name_key + self.yesterday, len(camDictList))
            self.lock.acquire()
            try:
                self.keynumber += len(camDictList)
            finally:
                self.lock.release()
            for camDict in camDictList:
                del camDict['negativeWords']
                del camDict['exactNegativeWords']
                mergeDataTem = {**camDict, **account}
                mergeData =  Utils().matchKeyChinese(mergeDataTem)
                """将广告组Id保存到redis里--->sogou_keyId_20180429,广告主id,广告组Id+关键词Id"""
                redis.zadd(self.redisKey + self.yesterday, '{}_{}'.format(mergeData['cpcGrpId'], mergeData[self.keyId]),
                           adgId[3])
                #redis.rpush('sogou_keyId_prepare_' + self.yesterday,
                #            '{}_{}'.format(mergeData['cpcGrpId'], mergeData[self.keyId]))
                redis.expire(self.redisKey + self.yesterday, 30 * 24 * 3600)
                """将广告主信息保存到hbase里--->0|20180428+accountid+adgid+keyid"""
                self.putKeyDataToHbase(adgId[3], mergeData['cpcGrpId'], mergeData[self.keyId], mergeData)
                Logger().inilg(self.interface_name_key,
                               '{}_{} update  is success {}'.format(adgId[3],mergeData['cpcGrpId'] ,mergeData[self.keyId]))
            redis.sadd('updatakeyId_vaild_accountId_' + self.yesterday, adgId[3])
            redis.expire('updatakeyId_vaild_accountId_' + self.yesterday, 30 * 24 * 3600)
        else:
            self.lock.acquire()
            try:
                self.keynull += 1
            finally:
                self.lock.release()
            Write().recordErrorLog(self.yesterday, adgId[2], self.interface_name_key, 10000, 'Data id null!')
            Logger().inilg('{}Null'.format(self.interface_name_key),
                           '{} update adgId is success but data is null {}'.format(adgId[2], xmlDict))

    def putKeyDataToHbase(self, *hbaseKey):
        self.lock.acquire()
        try:
            self.listEmpty.append((str(hbaseKey[0])[-1] + '|' + self.yesterday + '|' + str(hbaseKey[0]) + '|' + str(
                hbaseKey[1]) + '|' + str(hbaseKey[2]), hbaseKey[3]))
            if len(self.listEmpty) >= 5000:
                Conn().putDataToHbase(self.listEmpty, self.keyHbase)
        finally:
            self.lock.release()

    def executekeyFunction(self):
        # 记录开始日志到MySQL里
        # adgId--->"""代理商名称，代理商密码,广告主名称,广告主Id,广告组Id集合,token"""
        Write().start_info(self.yesterday, self.grpId + self.yesterday, self.interface_name_key)
        if len(adgAdvId) != 0:
            futures = []
            for adgId in adgAdvId:
                futures.append(ex.submit(self.getkeyData, adgId))
            wait(futures)
            Conn().putDataToHbase(self.listEmpty, self.keyHbase)
            Logger().inilg('{}Null'.format(self.interface_name_key),
                           'keyId data is null number is {}'.format(self.keynull))
            Logger().inilg(self.interface_name_key, 'keyData send hbase number is {}'.format(self.keynumber))
        else:
            Logger().inilg(self.interface_name_key, 'adgInformation is null,please check camId whether update!')


if __name__ == '__main__':
    """执行关键词程序"""
    Key().executekeyFunction()

    # 记录请求次数到MySQL里
    requestTime = redis.get(Key().service_name_key + Key().yesterday)
    Write().putApiStatisticsInfo(Key().interface_name_key, requestTime.decode())
    # 记录结束日志到MySQL里
    total = redis.get(Key().interface_name_key + Key().yesterday)
    Write().end_info(-1, total.decode(), Key().yesterday, Key().keyId + Key().yesterday, Key().interface_name_key)

    Logger().inilg(Key().interface_name_key, '----------------------------keyId update complete')
