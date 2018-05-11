#################################
# File Name: DailyKeywordReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class DailyKeyword(ReportHandler):

    # 拉取reportId的请求体
    def get_report_id_request(self, default_request):
        request_body = '''<v11:getReportId>
        <reportRequestType>
        <performanceData>cost</performanceData>
        <performanceData>cpc</performanceData>
        <performanceData>click</performanceData>
        <performanceData>impression</performanceData>
        <performanceData>ctr</performanceData>
        <performanceData>position</performanceData>
        <startDate>{}T00:00:00.000</startDate>
        <endDate>{}T23:59:59.999</endDate>
        <reportType>{}</reportType>
        <unitOfTime>{}</unitOfTime>
        </reportRequestType>
        </v11:getReportId>'''.format(self.report_start_date, self.report_end_date, self.report_type, self.unit_of_time)
        request = default_request.format(request_body)
        return request

    # 日报表关键词层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        date = fields[1]
        key_id = fields[7]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + cpcId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', ''), account_id, key_id)
        return row_key

    # 日报表关键词层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, key_id, key_word, cost, cpc, click, impression, \
        ctr, position = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'group_id': group_id, 'group': group, 'key_id': key_id, 'key_word': key_word, 'cost': cost,
                      'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr, 'position': position}
        return json.dumps(value_dict)

    # 有数据的账户id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        key = 'sogou_api_valid_daily_keyword_{}'.format(self.report_start_date.replace('-', ''))
        key_id = fields[7]
        value = '{}_{}_{}'.format(agent_user_name, account_id, key_id)
        r.sadd(key, value)

    # 处理结构不完整的json字符串
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, key_id, key_word, cost, cpc, click, impression, \
        ctr, position = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'group_id': group_id, 'group': group, 'key_id': key_id,
                      'key_word': key_word, 'cost': cost, 'cpc': cpc, 'click': click, 'impression': impression,
                      'ctr': ctr, 'position': position}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('daily_keyword_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    dkr = DailyKeyword()
    dkr.num_threads = 5
    dkr.table_name = 'sogou_keyword_daily_report'
    dkr.data_source_key = 'sogou_accountId_daily_valid_' + start_date.replace('-', '')
    dkr.report_start_date = start_date
    dkr.report_end_date = end_date
    dkr.report_type = '5'
    dkr.unit_of_time = '1'
    dkr.mysql_interface = 'ReportService(DailyKeyword)'
    dkr.start()
