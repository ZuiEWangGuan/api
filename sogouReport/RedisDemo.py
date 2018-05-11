from SoGouReportHandler import *

r = RedisConn.get_redis_conn()

#
# username_userpassed = r.hget('sogou_agencysname', '14')
# field2 = username_userpassed.decode('utf-8').split('_')
# agent_user_name = field2[0]
# agent_user_password = field2[1]
#
# print(agent_user_name)
# print(agent_user_password)

# print(r.scard('sogou_cam_id_20180421'))

# def demo(directory_name):
#     if not os.path.exists(directory_name):
#         os.mkdir(directory_name)
#         print('make directory succeed')
#     else:
#         print('directory was exists')
#
#
# if __name__ == '__main__':
#     demo('c:/company/SoGouDemo')

# r.delete('sogou_account_20180425')
# r.zadd('sogou_account_20180425', 'hancheng145@sina.cn##|19976158', '14')
# r.zadd('sogou_account_20180425', 'caohua@we.com##|20219965', '14')
# r.zadd('sogou_account_20180425', 'fushi@yhd.com##|20245894', '14')
# r.zadd('sogou_account_20180425', 'youqu586@sina.cn##|20203833', '14')
# r.zadd('sogou_account_20180425', 'szsy1334@gslrbl.cn##|20257716', '14')

date = Utils.get_yesterday()
print('日账户: ', r.scard('sogou_api_valid_daily_account_' + date))
print('日计划: ', r.scard('sogou_api_valid_daily_plan_' + date))
print('日推广组: ', r.scard('sogou_api_valid_daily_group_' + date))
print('日关键词: ', r.scard('sogou_api_valid_daily_keyword_' + date))
print('日创意: ', r.scard('sogou_api_valid_daily_idea_' + date))
print('日搜索词: ', r.scard('sogou_api_valid_daily_search_' + date))

# print(r.get('SoGou_minute_controller'))
# print(r.ttl('SoGou_minute_controller'))
print('小时账户: ', r.scard('sogou_api_valid_hourly_account_' + date))
print('小时计划: ', r.scard('sogou_api_valid_hourly_plan_' + date))
print('小时推广组: ', r.scard('sogou_api_valid_hourly_group_' + date))
print('小时关键词: ', r.scard('sogou_api_valid_hourly_keyword_' + date))

# sdiff = r.sdiff('sogou_api_valid_daily_account_demo1_20180502', 'sogou_api_valid_daily_account_demo2_20180502')
# for i in sdiff:
#     print(i.decode())
# print(len(sdiff))

# print(r.delete('sogou_api_valid_daily_account_' + date, 'sogou_api_valid_daily_group_' + date,
#                'sogou_api_valid_daily_idea_' + date, 'sogou_api_valid_daily_keyword_' + date,
#                'sogou_api_valid_daily_plan_' + date, 'sogou_api_valid_daily_search_' + date,
#                'sogou_api_valid_hourly_account_' + date, 'sogou_api_valid_hourly_group_' + date,
#                'sogou_api_valid_hourly_keyword_' + date, 'sogou_api_valid_hourly_plan_' + date))

print(r.zcard('sogou_accountId_' + date))

# print(r.keys())
