import requests
import threading
from handleconn import Conn
from sogouUtils import Utils
from loggers.logger import Logger
class ApiHandle:

    def __init__(self):

        """获取redis连接"""
        self.redis=Conn().redis_pool
        """日期格式为20180420"""
        self.yesterday=Utils().yeaterday
        """初始化一个锁"""
        self.lock = threading.Lock()
        """广告主信息空list"""
        self.allList=[]
        """广告主Id空list"""
        self.advList=[]
        """广告计划Id空List"""
        self.camList=[]
        """广告组Id空List"""
        self.adgList=[]
        self.advHbase = 'sogou_advertiser'
        self.camId=0
        self.adgId=0

    def getAgsAndAdv(self):
        """从redis获取代理商信息和广告主的名称"""
        agentusername=self.redis.hgetall('sogou_agencysname')
        for agsId,agsName in agentusername.items():
            """将redis里代理商名称和密码切出来"""
            splitAnsNameAndPw=str(agsName.decode()).split('_')
            agsNa=splitAnsNameAndPw[0]
            agsPw=splitAnsNameAndPw[1]
            """获取代理商token"""
            token=self.redis.hget('sogou_access_token',agsId.decode())
            allAccountName=self.redis.zrangebyscore('sogou_account_name_'+self.yesterday,agsId.decode(),agsId.decode())
            if len(allAccountName) != 0:
               for account in allAccountName:
                   """代理商ID，代理商名称，代理商密码,广告主名称,token"""
                   self.allList.append((agsId.decode(),agsNa,agsPw,account.decode(),token.decode()))
            else:
                Logger().inilg('advAndAgs'+self.yesterday,'{} 代理商没有广告主信息'.format(agsId.decode()))
        print(len(self.allList))
        return self.allList

    def getAdvId(self):
        """获取广告主ID"""#,'164231376@qq.com##|20501299'
        #a=['164231376@qq.com##|20501299']
        agentusername=self.redis.hgetall('sogou_agencysname')
        for agsId,agsNameAndPw in agentusername.items():
            """将redis里代理商名称和密码切出来"""
            splitAnsNameAndPw=str(agsNameAndPw.decode()).split('_')
            agsName=splitAnsNameAndPw[0]
            agsPw=splitAnsNameAndPw[1]
            """获取代理商token"""
            token=self.redis.hget('sogou_access_token',agsId.decode())
            allAccountNameAndId=self.redis.zrangebyscore('sogou_accountId_'+self.yesterday,agsId.decode(),agsId.decode())
            if len(allAccountNameAndId) != 0:
               #allAccountNameAndId
                for account in allAccountNameAndId:
                     """代理商ID，代理商名称，代理商密码,广告主名称+Id,token"""
                     self.advList.append((agsId.decode(),agsName,agsPw,account.decode(),token.decode()))
            else:
                Logger().inilg('adv' + self.yesterday, '{} 代理商没有广告主信息'.format(agsId.decode()))
        return self.advList

    def getCamId(self):
        advID=self.getAdvId()
        for accountAndAgs in advID:
            """将广告主name和id切出来"""
            splitAgsNameAndPw = str(accountAndAgs[3]).split('##|')
            accountName=splitAgsNameAndPw[0]
            accountId=splitAgsNameAndPw[1]
            """获取代理商Id和对应的计划Id"""
            allAgsIdAndAdvId = self.redis.zrangebyscore('sogou_camId_' + self.yesterday, accountId,accountId)
            if len(allAgsIdAndAdvId) != 0:
                camTags=[]
                for camId in allAgsIdAndAdvId:
                   """将代理商Id和广告计划Id切出来"""
                   splitAgsAndCam = str(camId.decode()).split('_')
                   campanId=splitAgsAndCam[1]
                   camTags.append(campanId)
                """代理商名称，代理商密码,广告主名称,广告主Id,广告计划Id集合,token"""
                self.camList.append((accountAndAgs[1], accountAndAgs[2],accountName
                                     ,accountId,camTags,accountAndAgs[4]))
            else:
                self.camId +=1
                Logger().inilg('cam','{} 广告主没有广告计划信息'.format(accountId))
        Logger().inilg('cam', '一共没有广告计划的广告主的数量为 {}'.format(self.camId))
        return self.camList

    def getAdgId(self):
        """获取所有的推广组ID---------->广告主id，广告计划id+广告组id"""
        advID=self.getAdvId()
        for accountAndAgs in advID:
            """将广告主name和id切出来"""
            splitAgsNameAndPw = str(accountAndAgs[3]).split('##|')
            accountName = splitAgsNameAndPw[0]
            accountId = splitAgsNameAndPw[1]
            allCamIdAndAdgId = self.redis.zrangebyscore('sogou_adgId_' + self.yesterday, accountId,accountId)
            if len(allCamIdAndAdgId) != 0:
                adgTags=[]
                for adgId in allCamIdAndAdgId:
                    """将广告计划Id和对应的广告组Id切出来"""
                    splitCamAndAdg = str(adgId.decode()).split('_')
                    cutAdgId=splitCamAndAdg[1]
                    adgTags.append(cutAdgId)
                """代理商名称，代理商密码,广告主名称,广告主Id,广告组Id集合,token"""
                self.adgList.append((accountAndAgs[1], accountAndAgs[2], accountName, accountId, adgTags,accountAndAgs[4]))
            else:
                self.adgId += 1
                Logger().inilg('adg', '{} 广告主没有广告组信息'.format(accountId))
        Logger().inilg('adg', '一共没有广告组的广告主的数量为 {}'.format(self.adgId))
        return self.adgList


    def sendHttpRequest(self,service_name,agsName,agsPw,userName,token,http_body):
        retryTime=0
        isSuccessApiCall = True
        """请求搜狗API"""
        """"服务名，代理商名，代理商密码，用户名，token，httpbody"""
        wsdl = 'http://api.agent.sogou.com:80/sem/sms/v1/' + service_name + '?wsdl'

        request_param = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://api.sogou.com/sem/common/v1" xmlns:v11="https://api.sogou.com/sem/sms/v1">
        <soapenv:Header>
            <v1:AuthHeader>
                <v1:agentusername>{}</v1:agentusername>
                <v1:agentpassword>{}</v1:agentpassword>
                <v1:username>{}</v1:username>
                <v1:token>{}</v1:token>
            </v1:AuthHeader>
        </soapenv:Header>
        {}
        </soapenv:Envelope>'''.format(agsName,agsPw,userName,token,http_body)
        #print(request_param)
        while isSuccessApiCall:
            try:
               isSuccessApiCall = False
               response = requests.post(wsdl, data=request_param)
               # 记录每个服务的请求次数
               self.redis.incrbyfloat(service_name + self.yesterday)

               xmlText = response.text

               return xmlText
            except Exception as e:
                self.lock.acquire()
                try:
                    retryTime += 1
                    Logger().inilg('httpexception{}'.format(service_name), '{}_{}_{}'.format(retryTime,userName, repr(e)))
                finally:
                    self.lock.release()
                if retryTime == 3:
                   isSuccessApiCall = False
                   Logger().inilg('httpexception{}'.format(service_name),'{}_{}_{}'.format(retryTime,userName,repr(e)))
                   self.redis.sadd(service_name,'{}_{}'.format(agsName,userName))



    #
    # def putAdvIdToRedis(self, redisKey, prefix, suffix, dataId):
    #     """zset--->key20180429,score,value"""
    #
    #     self.redis.zadd(redisKey + self.yesterday, '{}##|{}'.format(prefix, suffix), dataId)
    #
    #
    # def putIdToRedis(self, redisKey, prefix, suffix, dataId):
    #     """zset--->key20180429,score,value"""
    #
    #     self.redis.zadd(redisKey + self.yesterday, '{}_{}'.format(prefix, suffix), dataId)












