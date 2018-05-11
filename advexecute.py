import threading
from concurrent import futures
from concurrent.futures import wait
from sogou_apihandle import ApiHandle
from readmysqldata import Write
from sogouUtils import Utils
from handleconn import Conn
from loggers.logger import Logger

"""初始化线程池"""
ex=futures.ThreadPoolExecutor(max_workers=4)
"""初始化redis"""
redis = Conn().redis_pool
"""初始化代理商信息"""
agsAndAdv = ApiHandle().getAgsAndAdv()


class Adv:

    def __init__(self):
        self.service_name_adv = 'AccountService'
        self.interface_name_adv = 'getAccountInfo'
        self.accountId = 'accountid'
        self.yesterday=Utils().yeaterday
        self.advHbase = 'sogou_advertiser'
        """初始化一个锁"""
        self.lock = threading.Lock()
        self.advnumber = 0
        self.advnull=0
        self.advexception = 0
        self.listEmpty=[]
        self.redisKey='sogou_accountId_'

    def getAdvData(self,list,body):
        """遍历所有的广告主信息"""
        isSuccessApiCall = True
        retryCount = 0
        while isSuccessApiCall:
            """"传入的参数：服务名，代理商名，代理商密码，用户名，token，httpbody"""
            xml = ApiHandle().sendHttpRequest(self.service_name_adv, list[1],list[2], list[3], list[4], body)
            """转为字典格式"""
            xmlDictTem = Utils().xMLParser(xml)
            if xmlDictTem['ns3:desc'] == 'success':
                if self.accountId in xmlDictTem.keys():
                       """将regions标签的内容读出来解析为数组，替换xmlDict的regions"""
                       regionsList = Utils().xmlParseAdvRegions(xml)
                       string = Utils().listToString(regionsList)
                       xmlDictTem['regions'] = string
                       """剔除json里无效的数据"""
                       xmlDict = Utils().tagAdvConversion(xmlDictTem)
                       """将广告主Id保存到redis里--->sogou_accountId_20180429,代理商Id，广告主name+广告主id"""
                       redis.zadd(self.redisKey + self.yesterday, '{}##|{}'.format(list[3], xmlDict[self.accountId]),  list[0])
                       redis.expire(self.redisKey + self.yesterday, 30 * 24 * 3600)
                       """将广告主信息保存到hbase里--->0|20180428+accountid"""
                       self.putAdvDataToHbase(xmlDict[self.accountId], xmlDict)
                       self.lock.acquire()
                       try:
                         self.advnumber += 1
                       finally:
                           self.lock.release()
                       redis.incrbyfloat(self.interface_name_adv + self.yesterday)
                       Logger().inilg(self.interface_name_adv,'{} update account is success {}'.format(list[3], xmlDict[self.accountId]))
                       redis.sadd('updataadvId_vaild_accountId_' + self.yesterday, xmlDict[self.accountId])
                       isSuccessApiCall = False
                else:
                    self.lock.acquire()
                    try:
                       self.advnull += 1
                    finally:
                        self.lock.release()
                    """返回成功,但没有有效的数据"""
                    Write().recordErrorLog(Utils().yeaterday, list[3], self.interface_name_adv, 10000, 'Data id null!')
                    Logger().inilg('{}Null'.format(self.interface_name_adv),
                                   '{} update accountId is success but data is null {}'.format(list[3], xml))
                    isSuccessApiCall = False
            else:
                self.lock.acquire()
                try:
                   retryCount += 1
                   Logger().inilg('{}Exception'.format(self.interface_name_adv),'{} retry {} 次'.format(list[3], retryCount))
                finally:
                    self.lock.release()
                if retryCount == 3:
                    self.lock.acquire()
                    try:
                       self.advexception += 1
                       isSuccessApiCall = False
                       Logger().inilg('{}Exception'.format(self.interface_name_adv),
                                      '{} retry come up to 3 次,Record the AD master record to MySQL {} .'.format(list[3],xmlDictTem))
                       Write().recordErrorLog(self.yesterday, list[3], self.interface_name_adv, xmlDictTem['ns3:code'],
                                           str(xmlDictTem['ns3:message']))
                    finally:
                        self.lock.release()

    def putAdvDataToHbase(self, *hbaseKey):
        self.lock.acquire()
        try:
           self.listEmpty.append((str(hbaseKey[0])[-1] + '|' + self.yesterday + '|' +str(hbaseKey[0]), hbaseKey[1]))
           if len(self.listEmpty) >= 5000:
               Conn().putDataToHbase(self.listEmpty, self.advHbase)
               del self.listEmpty[:]
        finally:
            self.lock.release()

    def executeAdvFunction(self):
        # 记录开始日志到MySQL里
        Write().start_info(self.yesterday, 'sogou_accountId_{}'.format(self.yesterday), self.interface_name_adv)
        futures = []
        if len(agsAndAdv) != 0:
           bodyAdv = """
           <soapenv:Body>
               <v11:{}/>
           </soapenv:Body>
           """.format(self.interface_name_adv)
           for advId in agsAndAdv:
               futures.append(ex.submit(self.getAdvData,advId,bodyAdv))
           wait(futures)
           Conn().putDataToHbase(self.listEmpty, self.advHbase)
           Logger().inilg(self.interface_name_adv, 'advData send hbase {} number'.format(self.advnumber))
           Logger().inilg('{}Null'.format(self.interface_name_adv),'account data is null number is {}'.format(self.advnull))
           Logger().inilg('{}Exception'.format(self.interface_name_adv),'account exception number is {}'.format(self.advexception))
        else:
          Logger().inilg(self.interface_name_adv,'accountInformation is null,please check account whether update!')

if __name__ == '__main__':

    Adv().executeAdvFunction()
    # 记录请求次数到MySQL里
    requestTime = redis.get(Adv().service_name_adv + Adv().yesterday)
    Write().putApiStatisticsInfo(Adv().interface_name_adv, requestTime.decode())
    # 记录结束日志到MySQL里
    total = redis.get(Adv().interface_name_adv + Adv().yesterday)
    Write().end_info(-1, total.decode(), Adv().yesterday, Adv().accountId + Adv().yesterday, Adv().interface_name_adv)

    Logger().inilg(Adv().interface_name_adv, '----------------------------accountId update complete')