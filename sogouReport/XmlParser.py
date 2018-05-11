# -*- coding: utf-8 -*-

import xml.sax
import xml.sax.handler


class XMLHandler(xml.sax.handler.ContentHandler):

    def __init__(self):
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


class XMLParser:

    def __init__(self):
        self.xh = XMLHandler()

    def parse(self, xml_str):
        xml.sax.parseString(xml_str, self.xh)
        res_dict = self.xh.getDict()
        return res_dict


if __name__ == '__main__':
    data = '''
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Header><ns3:ResHeader xmlns:ns3="http://api.sogou.com/sem/common/v1" xmlns:ns2="https://api.sogou.com/sem/sms/v1"><ns3:desc>success</ns3:desc><ns3:oprs>1</ns3:oprs><ns3:oprtime>0</ns3:oprtime><ns3:quota>1</ns3:quota><ns3:rquota>1997068806</ns3:rquota><ns3:status>0</ns3:status></ns3:ResHeader></soap:Header><soap:Body><ns2:getAccountInfoResponse xmlns:ns3="http://api.sogou.com/sem/common/v1" xmlns:ns2="https://api.sogou.com/sem/sms/v1"><accountInfoType><accountid>426116</accountid><balance>45639.81</balance><totalCost>4.881562596E7</totalCost><totalPay>4.886126577E7</totalPay><regions>1000</regions><regions>2000</regions><regions>3000</regions><regions>33000</regions><regions>9000</regions><regions>5000</regions><regions>11000</regions><regions>4000</regions><regions>12000</regions><regions>10000</regions><regions>8000</regions><regions>13000</regions><regions>14000</regions><regions>15000</regions><regions>16000</regions><regions>17000</regions><regions>18000</regions><regions>19000</regions><regions>20000</regions><regions>21000</regions><regions>22000</regions><regions>23000</regions><regions>24000</regions><regions>25000</regions><regions>26000</regions><regions>27000</regions><regions>28000</regions><regions>29000</regions><regions>30000</regions><regions>31000</regions><regions>32000</regions><domains>http://www.360buy.com/</domains><domains>http://www.jd.com/</domains><domains>http://www.jd.hk/</domains><ns3:opt><ns3:optInt><ns3:key>accountLevel</ns3:key><ns3:value>4</ns3:value></ns3:optInt></ns3:opt></accountInfoType></ns2:getAccountInfoResponse></soap:Body></soap:Envelope>
    '''
    dict = XMLParser().parse(data)
    print(str(dict))
    print(dict['balance'])
