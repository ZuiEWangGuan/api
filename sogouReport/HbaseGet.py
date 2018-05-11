import logging
from configparser import ConfigParser

from _datetime import datetime
import happybase
import re

cfp = ConfigParser()
cfp.read('SoGou.ini', encoding='utf-8')

hbase_host = cfp.get('hbase', 'hbase_host')

connection = happybase.Connection(host=hbase_host)
# print(connection.tables())
# table = connection.table('gdt_campaigns')
table = connection.table('sogou_account_hourly_report_single')

# print(table.row('8|2018050721|20239428'))
# families = table.families()
# for data in families:
#     print(data)

# for key, value in table.scan(row_prefix='hur2018041700AD', row_start='hur20180417', row_stop='hur20180418'):
start_time = datetime.now()
scan = table.scan()
# print(type(scan))
i = 0
for key, value in scan:
    print(key, value)
    i += 1
print('i =', i)
print('run time:', datetime.now() - start_time)

# row_key = 'hur20180424AD6584485544880083'
# row_key = 'hur20180424AD6609348269437723'
# row = table.row(row_key)
# print(row_key)
# print(row)

# connection.close()

# scan 'gdt_campaigns', {COLUMNS => 'info', STARTROW => 'cam20180409', ENDROW => 'cam20180409'}
# logs = table.scan(columns='info', row_start='cam20180409', row_stop='cam20180410')
# print(logs)
# print(type(logs))
#
# for log in logs:
#     print(log)

# row = table.row('cam20180408729725728105072', columns=['info:data'])
# print(row)

# for key, data in logs:
#     print(str(key))
#     print(type(str(key)))
#     # dataNew = data['info:data']['campaign_name']
#     dataNew = data
#     typeData = type(dataNew)
#     print(typeData)
#     print(dataNew)
#     logger().debug(data)
#     print('----------------')
#     # print(key, str(data))

# for k, v in logs.items():
#     print(v)

# for key, data in table.scan():
#     print(key, data)

# families = table.families()
# f = open('temp_logs.txt', 'a')
# for data in families:
#     print(data)
#     if len(re.findall(r'[2018]', data.decode())) != 0:
#         print(data)
