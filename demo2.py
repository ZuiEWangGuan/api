import re
from handleconn import Conn

# for validAccount in validAccountAll:
#     #获取所有的广告计划ID
#     AllcampaignId = pool.zrangebyscore('api_campaign_id_' + yestoday,
#                                        str(agencysId.decode()) + '00' + str(validAccount.decode()).split('_')[
#                                            1],
#                                        str(agencysId.decode()) + '00' + str(validAccount.decode()).split('_')[
#                                            1])
#     #将广告计划以1：100的数据发送到API请求
#
#     for dicingcutAllcampaignId in cutAllcampaignId:
#         for campaignId in dicingcutAllcampaignId:
#             levelId.append(AllcampaignId[campaignId].decode())
#         DailyReports().traverseLevel('daily_reports/get', token.decode(),
#                                      str(validAccount.decode()).split('_')[1],
#                                      agencysId.decode(), str(day[0]), str(day[1]), 'CAMPAIGN', levelId)
#         del levelId[:]
#     #不足100的另外请求
#
#
#
#         for residuecampaignId in residueAllcampaignId:
#             levelId.append(residuecampaignId.decode())
#         DailyReports().traverseLevel('daily_reports/get', token.decode(),
#                                      str(validAccount.decode()).split('_')[1],
#                                      agencysId.decode(), str(day[0]), str(day[1]), 'CAMPAIGN', levelId)
#         del levelId[:]
# DailyReports().batch[1].close()
# requests = pool.get('daily_reports/get'.replace('/', '') + 'CAMPAIGN' + yestoday)
# Write().putApiStatisticsInfo('daily_reports/get/CAMPAIGN', int(requests))

#2282
pool=Conn().getRedisConn()
#pool.delete('sogou_accountId_20180509')
#pool.delete('sogou_adgId_20180509')
#pool.delete('sogou_camId_20180509')
#pool.delete('sogou_camId_prepare_20180509')
#pool.delete('sogou_adgId_prepare_20180509')
#pool.delete('updataadgId_vaild_accountId_20180509')
#pool.delete('sogou_keyId_20180509')
#pool.delete('sogou_keyId_prepare_20180509')
#sogou_camId_prepare_
#sogou_keyId_
#sogou_adcId_prepare_
#q=pool.zcard('sogou_accountId_20180509')
#l=pool.zcard('sogou_camId_20180509')
#y=pool.llen('sogou_camId_prepare_20180509')
#t=pool.zcard('sogou_adgId_20180509')
#p=pool.zcard('sogou_adgId_test_20180509')
#k=pool.llen('sogou_adgId_prepare_20180509')
#m=pool.scard('updataadgId_vaild_accountId_20180509')
#t=pool.zcard('sogou_adcId_20180509')
##p=pool.zcard('sogou_adgId_test_20180509')
#k=pool.llen('sogou_adcId_prepare_20180509')
t=pool.zcard('sogou_keyId_20180510')
#p=pool.zcard('sogou_adgId_test_20180509')
#updatakeyId_vaild_accountId_
#k=pool.llen('sogou_adcId_prepare_20180510')
m=pool.scard('updatakeyId_vaild_accountId_20180510')
#updataadgId_vaild_accountId_
#o=pool.scard('1111111')
#print(q)
#print(l)
#print(y)
print(t)
#print(p)
#print(k)
print(m)
#h=pool.zrangebyscore('sogou_accountId_20180508',14,14)
#for j in h:
#    a=str(j.decode()).split('##|')[1]
#    pool.sadd('666666',a)
#w=pool.scard('666666')
#print(w)
#print(o)
# for a in k:
#     g=str(a.decode()).split('-')
#     #print(g)

u=''

#pattern = re.compile(r'[\u4E00-\u9FA5]|[a-zA-Z0-9]')
rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5'\"\- ]")
#result_list = re.findall('[a-zA-Z0-9]+',u)
#print(str(pattern.findall(u)).replace(',','').replace("'",'').replace(' ',''))












