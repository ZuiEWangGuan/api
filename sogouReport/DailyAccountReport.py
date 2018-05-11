#################################
# File Name: DailyAccountReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-24
#################################


from SoGouReportHandler import *
from loggerConfig import Loggers


class DailyAccount(ReportHandler):

    # 日报表账户层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        date = fields[1]
        # row_key: id最后一位 + | + yyyyMMdd + accountId
        row_key = '{}|{}|{}'.format(account_id[-1:], date.replace('-', ''), account_id)
        return row_key

    # 日报表账户层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'cost': cost, 'cpc': cpc,
                      'click': click, 'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)

    # 有数据的账户id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        date = fields[1]
        key = 'sogou_api_valid_daily_account_{}'.format(date.replace('-', ''))
        value = '{}_{}'.format(agent_user_name, account_id)
        r.sadd(key, value)
        """以下输出，供日报其他层级使用"""
        key = 'sogou_accountId_daily_valid_{}'.format(date.replace('-', ''))
        value = '{}##|{}'.format(user_name, account_id)
        score = agency_id
        r.zadd(key, value, score)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('daily_account_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    dar = DailyAccount()
    dar.num_threads = 5
    dar.table_name = 'sogou_account_daily_report'
    dar.data_source_key = 'sogou_accountId_' + start_date.replace('-', '')
    dar.report_start_date = start_date
    dar.report_end_date = end_date
    dar.report_type = '1'
    dar.unit_of_time = '1'
    dar.mysql_interface = 'ReportService(DailyAccount)'
    dar.start()
