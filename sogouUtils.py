import time
import copy
import xml.sax
import xml.sax.handler
import re
import numpy
import json,threading
from datetime import timedelta, datetime
from loggers.logger import Logger


class XMLHandler(xml.sax.handler.ContentHandler):

    def __init__(self):
        """实现ContentHandler接口"""
        super().__init__()
        self.buffer = ""
        self.mapping = {}

    def startElement(self, name, attributes):
        self.buffer = ""

    def characters(self, data):
        self.buffer += data

    def endElement(self, name):
        self.mapping[name] = self.buffer

    def getDict(self):
        return self.mapping


class Utils:

    def __init__(self):
        self.yeaterday=self.getYesterday()
        self.yeaterdayV2 = self.getYesterdayV2()
        self.xh = XMLHandler()
        """初始化一个锁"""
        self.lock = threading.Lock()

    # 获取前一天日期
    def getYesterday(self):
        """格式20180502"""
        yesterday = datetime.today() + timedelta(-1)
        return yesterday.strftime("%Y%m%d")

    def getYesterdayV2(self):
        """格式2018-05-02"""
        yesterday = datetime.today() + timedelta(-1)
        return yesterday.strftime("%Y-%m-%d")

    def listToString(self,list):
        """将list转变为，分割的字符串"""
        string=','.join(list)
        return string

    def joinString(self,prefix,tagString):
        """对字符串拼接一个标签"""
        joinTags = '<{}>{}</{}>'.format(prefix,tagString,prefix)
        return joinTags

    def tags(self,tag):
        """添加标签，以便可以解析为字符串"""
        headle = '<ns2:getResponse xmlns:ns3="http://api.sogou.com/sem/common/v1" ' \
            'xmlns:ns2="https://api.sogou.com/sem/sms/v1">{}</ns2:getResponse>'.format(tag)
        return headle

    def xMLParser(self,xmlString):
        """将xml字符串解析为字典"""
        xmlParse=xml.sax.parseString(xmlString,self.xh)
        res_dict = self.xh.getDict()
        return res_dict

    def xmlParserHeader(self,xml):
        """将xml头信息解析出来"""
        header=self.xmlParserList(xml,'<soap:Header>','</soap:Header>')
        for head in header:
           headDict=self.xMLParser(head)
           """删除无效数据"""
           del headDict['ns3:ResHeader']
           return headDict

    def xmlParserList(self,response,prefix,suffix):
        """正则匹配数据"""
        com = re.compile(r'{}(.*?){}'.format(prefix,suffix), re.S)
        searchObj = com.findall(response)
        return searchObj

    def xmlParseAdvRegions(self,xml):
        """将regions解析为数组"""
        xmlListRegions=[]
        xmlList = self.xmlParserList(xml, '<regions>', '</regions>')
        for list in xmlList:
            naga='<regions>{}</regions>'.format(list)
            words = self.xMLParser(naga)
            xmlListRegions.append(words['regions'])
        return xmlList


    def tagAdvConversion(self, xmlDict):
        """删除广告主里无效的json数据"""
        del xmlDict['ns3:ResHeader']
        del xmlDict['soap:Header']
        del xmlDict['ns3:optInt']
        del xmlDict['ns3:opt']
        del xmlDict['ns2:getAccountInfoResponse']
        del xmlDict['soap:Body']
        del xmlDict['soap:Envelope']
        del xmlDict['accountInfoType']
        return xmlDict

    def xmlParserAll(self,parseList):
            """negativeWords有多个的数组"""
            """list会重复,向list里添加字典，在一个内存地址会重复，用copy包"""
            tagsList=[]
            for list in parseList:
                tag=self.tags(list)
                parse = self.xMLParser(tag)
                replaceParse=self.tagCamConversion(list,parse)
                copyParse=copy.copy(replaceParse)
                del copyParse['ns2:getResponse']
                tagsList.append(copyParse)
            return tagsList


    def tagCamConversion(self,string,parse):
        """推广计划列表------->对<negativeWords>和<exactNegativeWords>标签进行拆解为[,,,]格式，替换原来的单个值"""

        tagNega = self.xmlParserList(string, '<negativeWords>', '</negativeWords>')
        tagExact = self.xmlParserList(string, '<exactNegativeWords>', '</exactNegativeWords>')

        resTagNega = self.listToString(tagNega)
        resTagExact = self.listToString(tagExact)

        joinTagNega=self.joinString('negativeWords',resTagNega)
        joinTagExact=self.joinString('exactNegativeWords', resTagExact)

        xmlTagNega = self.xMLParser(joinTagNega)
        xmlTagExact = self.xMLParser(joinTagExact)

        parse['negativeWords'] = xmlTagNega['negativeWords']
        parse['exactNegativeWords'] = xmlTagExact['exactNegativeWords']

        return parse


    def allotList(self, dataList):
        """按一定比例来切分数量"""
        cutAllDataList = numpy.reshape(range(len(dataList) - len(dataList) % 100),
                                       (len(dataList) // 100, 100))
        return cutAllDataList

    def allotResidueList(self, dataList):
        """将不足一定比例的list信息返回"""
        start = len(dataList) % 100
        if int(start) != 0:
            residueAllDataList = dataList[len(dataList) - len(dataList) % 100:]
            return residueAllDataList
        else:
            return '0'


    def matchCamChinese(self,dict):
        try:
            string=json.dumps(dict)
            jsonDict=json.loads(string)
            return dict
        except:
           rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5'\"\- ]")
           valueString=rule.sub('', dict['cpcPlanName'])
           dict['cpcPlanName']=valueString
           Logger().inilg('cpcPlanNameexception',dict['cpcPlanName'])
           return dict

    def matchAdgChinese(self,dict):
        try:
            string=json.dumps(dict)
            jsonDict=json.loads(string)
            return dict
        except:
           rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5'\"\- ]")
           valueString=rule.sub('', dict['cpcGrpName'])
           dict['cpcGrpName']=valueString
           Logger().inilg('cpcGrpNameexception',dict['cpcGrpName'])
           return dict

        #valueString=str(dict['cpcGrpName']).replace("'",'').replace('"','\\\"')
        #Logger().inilg('cpcGrpNameexception',dict['cpcGrpName'])
        #Logger().inilg('cpcGrpNameexception', valueString)
        #return dict

    def matchKeyChinese(self, dict):
        try:
            string = json.dumps(dict)
            jsonDict = json.loads(string)
            return dict
        except:
            rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5'\"\- ]")
            valueString = rule.sub('', dict['cpc'])
            dict['cpc'] = valueString
            Logger().inilg('cpcexception', dict['cpc'])
            return dict

    def matchAdcChinese(self, dict):
        try:
            string = json.dumps(dict)
            jsonDict = json.loads(string)
            return dict
        except:
            rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5'\"\- ]")
            valueString = rule.sub('', dict['title'])
            dict['title'] = valueString
            valueString = rule.sub('', dict['description1'])
            dict['description1'] = valueString
            Logger().inilg('titleexception', dict['title']+':'+dict['description1'])
            return dict