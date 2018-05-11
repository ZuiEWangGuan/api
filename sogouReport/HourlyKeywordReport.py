#################################
# File Name: HourlyKeywordReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class HourlyKeyword(ReportHandler):

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

    @staticmethod
    def row_key_design_single(fields, account_id):
        date = fields[1]
        key_id = fields[7]
        # row_key: id最后一位 + | + yyyyMMdd + accountId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', '').replace(' ', ''), account_id, key_id)
        return row_key

    # 小时报表关键词层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        key_id = fields[0]
        date = fields[1]
        """row_key: id最后一位 + | + yyyyMMdd + accountId + key_id"""
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date, account_id, key_id)
        return row_key

    # 小时报表关键词层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, key_id, key_word, cost, cpc, click, impression, \
        ctr, position = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'group_id': group_id, 'group': group, 'key_id': key_id, 'key_word': key_word, 'cost': cost,
                      'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr, 'position': position}
        key_and_value = ('{}_{}'.format(key_id, date.replace('-', '')[:-3]), json.dumps(value_dict))
        return key_and_value

    # 有数据的关键词id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        key_id = fields[0]
        date = fields[1]
        key = 'sogou_api_valid_hourly_keyword_{}'.format(date)
        value = '{}_{}_{}'.format(agent_user_name, account_id, key_id)
        r.sadd(key, value)

    # 处理结构不完整的json数据
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, key_id, key_word, cost, cpc, click, impression, \
        ctr, position = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'group_id': group_id, 'group': group,
                      'key_id': key_id, 'key_word': key_word, 'cost': cost, 'cpc': cpc, 'click': click,
                      'impression': impression, 'ctr': ctr, 'position': position}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('hourly_keyword_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    hkr = HourlyKeyword()
    hkr.num_threads = 5
    hkr.save_mode = 4
    hkr.table_name = 'sogou_keyword_hourly_report'
    hkr.table_name_single = 'sogou_keyword_hourly_report_single'
    hkr.data_source_key = 'sogou_accountId_hourly_valid_' + start_date.replace('-', '')
    hkr.report_start_date = Utils.get_yesterday_by_point()
    hkr.report_end_date = Utils.get_yesterday_by_point()
    hkr.report_type = '5'
    hkr.unit_of_time = '4'
    hkr.mysql_interface = 'ReportService(HourlyKeyword)'
    hkr.start()
