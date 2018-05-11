import redis
from SoGouReportHandler import Utils
from Connections import RedisConn

if __name__ == '__main__':
    r = RedisConn.get_redis_conn()
    date = Utils.get_yesterday()
    daily_account = '日账户: ' + r.scard('sogou_api_valid_daily_account_' + date)
    hourly_account = '小时账户: ' + r.scard('sogou_api_valid_hourly_account_' + date)
    daily_plan = '日计划: ' + r.scard('sogou_api_valid_daily_plan_' + date)
    hourly_plan = '小时计划: ' + r.scard('sogou_api_valid_hourly_plan_' + date)
    daily_group = '日推广组: ' + r.scard('sogou_api_valid_daily_group_' + date)
    hourly_group = '小时推广组: ' + r.scard('sogou_api_valid_hourly_group_' + date)
    daily_keyword = '日关键词: ' + r.scard('sogou_api_valid_daily_keyword_' + date)
    hourly_keyword = '小时关键词: ' + r.scard('sogou_api_valid_hourly_keyword_' + date)
    daily_idea = '日创意: ' + r.scard('sogou_api_valid_daily_idea_' + date)
    daily_search = '日搜索词: ' + r.scard('sogou_api_valid_daily_search_' + date)

    count_list = [daily_account, hourly_account, daily_plan, hourly_plan, daily_group, hourly_group, daily_keyword,
                  hourly_keyword, daily_idea, daily_search]

    count_str = '\r\n'.join(count_list)

    r = redis.Redis(host='123.207.149.249', port=7000, password='J78kFi2Lfg4110k4OnfSA4FgsP254c')
    r.publish('wx_Dixon', count_str)
