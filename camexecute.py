import threading
from concurrent import futures
from concurrent.futures import wait
from sogou_apihandle import ApiHandle
from readmysqldata import Write
from sogouUtils import Utils
from handleconn import Conn
from loggers.logger import Logger

"""初始化线程池"""
ex = futures.ThreadPoolExecutor(max_workers=4)
"""初始化redis"""
redis = Conn().redis_pool
"""初始化代理商信息"""
advAndId = ApiHandle().getAdvId()

class Cam:

    def __init__(self):
        self.service_name_cam = 'CpcPlanService'
        self.interface_name_cam = 'getAllCpcPlan'
        self.camId = 'cpcPlanId'
        self.yesterday = Utils().yeaterday
        """初始化一个锁"""
        self.lock = threading.Lock()
        self.camnumber = 0
        self.camnull = 0
        self.camexception = 0
        self.camId = 'cpcPlanId'
        self.camHbase = 'sogou_campaigns'
        self.listEmpty = []
        self.redisKey = 'sogou_camId_'

    def getCamData(self, dataList, body):
        """遍历所有的广告主信息"""
        isSuccessApiCall = True
        retryCount = 0
        while isSuccessApiCall:
            advName = str(dataList[3]).split('##|')[0]
            accountId = str(dataList[3]).split('##|')[1]
            """"传入的参数：服务名，代理商名，代理商密码，用户名，token，httpbody"""
            xml = ApiHandle().sendHttpRequest(self.service_name_cam, dataList[1],dataList[2], advName, dataList[4], body)
            """转为字典格式"""
            xmlDict = Utils().xMLParser(xml)
            if xmlDict['ns3:desc'] == 'success':
                if self.camId in xmlDict.keys():
                    """解析标签头信息"""
                    headDict = Utils().xmlParserHeader(xml)
                    """将cpcPlanTypes标签里的数据取出来"""
                    camIdTagsList = Utils().xmlParserList(xml, '<cpcPlanTypes>', '</cpcPlanTypes>')
                    """将每一个标签内容解析为dict格式"""
                    camDictList = Utils().xmlParserAll(camIdTagsList)
                    redis.incr(self.interface_name_cam + self.yesterday, len(camDictList))
                    """记录有多少数据"""
                    self.lock.acquire()
                    try:
                        self.camnumber += len(camDictList)
                    finally:
                        self.lock.release()
                    for camDict in camDictList:
                        """将数据都和header合并一次"""
                        mergeDataTem = {**headDict, **camDict}
                        mergeData = Utils().matchCamChinese(mergeDataTem)
                        """将广告计划Id保存到redis里--->sogou_camId_20180429,广告主id，代理商Id+广告计划id"""
                        #ApiHandle().putIdToRedis(self.redisKey, dataList[0], mergeData[self.camId], accountId)
                        redis.zadd(self.redisKey + self.yesterday, '{}_{}'.format(dataList[0],  mergeData[self.camId]), accountId)
                        redis.expire(self.redisKey + self.yesterday,30*24*3600)
                        """将广告主信息保存到hbase里--->0|20180428+accountid+camid"""
                        self.putCamDataToHbase(accountId, mergeData[self.camId],mergeData)
                        Logger().inilg(self.interface_name_cam,'{}_{} update camId is success {}'.format(advName,accountId,mergeData[self.camId]))
                    """将有效的广告主id记录到redis里"""
                    redis.sadd('updatacamId_vaild_accountId_' + self.yesterday, accountId)
                    redis.expire('updatacamId_vaild_accountId_' + self.yesterday,30*24*3600)
                else:
                    self.lock.acquire()
                    try:
                        self.camnull += 1
                    finally:
                        self.lock.release()
                    # """返回成功,但没有有效的数据"""
                    Write().recordErrorLog(Utils().yeaterday, advName, self.interface_name_cam, 10000, 'Data id null!')
                    Logger().inilg('{}Null'.format(self.interface_name_cam),
                                   '{} update camId is success but data is null {}'.format(advName, xml))
                isSuccessApiCall = False
            else:
                self.lock.acquire()
                try:
                    retryCount += 1
                finally:
                    self.lock.release()
                Logger().inilg('{}Exception'.format(self.interface_name_cam),
                               '{} retry {} 次'.format(advName, retryCount))
                if retryCount == 3:
                    self.lock.acquire()
                    try:
                        self.camexception += 1
                    finally:
                        self.lock.release()
                    isSuccessApiCall = False
                    Logger().inilg('{}Exception'.format(self.interface_name_cam),
                                   '{} retry come up to 3 次,Record the AD master record to MySQL.'.format(advName))
                    Write().recordErrorLog(Utils().yeaterday, advName, self.interface_name_cam, xmlDict['ns3:code'],
                                           str(xmlDict['ns3:message']))

    def putCamDataToHbase(self, *hbaseKey):
        self.lock.acquire()
        try:
           self.listEmpty.append((str(hbaseKey[0])[-1] + '|' + self.yesterday + '|' + str(hbaseKey[0])+ '|' + str(hbaseKey[1]), hbaseKey[2]))
           if len(self.listEmpty) >= 5000:
               Conn().putDataToHbase(self.listEmpty, self.camHbase)
               del self.listEmpty[:]
        finally:
            self.lock.release()

    def executeCamFunction(self):
        # 记录开始日志到MySQL里
        Write().start_info(self.yesterday, 'sogou_camId_{}'.format(self.yesterday), self.interface_name_cam)
        futures = []
        if len(advAndId) != 0:
            bodyCam = """
           <soapenv:Body>
               <v11:{}/>
           </soapenv:Body>
           """.format(self.interface_name_cam)
            for advId in advAndId:
                futures.append(ex.submit(self.getCamData, advId, bodyCam))
            wait(futures)
            Conn().putDataToHbase(self.listEmpty,self.camHbase)
            Logger().inilg(self.interface_name_cam, 'camData send hbase number is {}'.format(self.camnumber))
            Logger().inilg('{}Null'.format(self.interface_name_cam),'camId data is null number is {}'.format(self.camnull))
            Logger().inilg('{}Exception'.format(self.interface_name_cam),'camId exception number is {}'.format(self.camexception))
        else:
            Logger().inilg(self.interface_name_cam, 'accountInformation is null,please check account whether update!')

if __name__ == '__main__':

    Cam().executeCamFunction()

    #记录请求次数到MySQL里
    requestTime = redis.get(Cam().service_name_cam + Cam().yesterday)
    Write().putApiStatisticsInfo(Cam().interface_name_cam, requestTime.decode())
    #记录结束日志到MySQL里
    total = redis.get(Cam().interface_name_cam + Cam().yesterday)
    Write().end_info(-1, total.decode(), Cam().yesterday, Cam().camId + Cam().yesterday, Cam().service_name_cam)
    Logger().inilg(Cam().interface_name_cam, '----------------------------camId update complete')
