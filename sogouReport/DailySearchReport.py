#################################
# File Name: DailySearchReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class DailySearch(ReportHandler):
    # 拉取reportId的请求体
    def get_report_id_request(self, default_request):
        request_body = '''<v11:getReportId>
        <reportRequestType>
        <performanceData>cost</performanceData>
        <performanceData>cpc</performanceData>
        <performanceData>click</performanceData>
        <startDate>{}T00:00:00.000</startDate>
        <endDate>{}T23:59:59.999</endDate>
        <reportType>{}</reportType>
        <unitOfTime>{}</unitOfTime>
        </reportRequestType>
        </v11:getReportId>'''.format(self.report_start_date, self.report_end_date, self.report_type, self.unit_of_time)
        request = default_request.format(request_body)
        return request

    # 日报表账户层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        date = fields[1]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + cpcId
        row_key = '{}|{}|{}'.format(account_id[-1:], date.replace('-', ''), account_id)
        return row_key

    # 日报表账户层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, idea_id, idea_title, idea_describe_1, \
        idea_describe_2, idea_url_visit, idea_url_show, idea_url_visit_moved, idea_url_show_moved, key_word, \
        search_word, cost, cpc, click, match = fields

        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': plan, 'group_id': group_id, 'group': group, 'idea_id': idea_id, 'idea_title': idea_title,
                      'idea_describe_1': idea_describe_1, 'idea_describe_2': idea_describe_2,
                      'idea_url_visit': idea_url_visit, 'idea_url_show': idea_url_show,
                      'idea_url_visit_moved': idea_url_visit_moved, 'idea_url_show_moved': idea_url_show_moved,
                      'key_word': key_word, 'search_word': search_word, 'cost': cost, 'cpc': cpc, 'click': click,
                      'match': match}
        return json.dumps(value_dict)

    # 有数据的账户id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        key = 'sogou_api_valid_daily_search_{}'.format(self.report_start_date.replace('-', ''))
        value = '{}_{}'.format(agent_user_name, account_id)
        r.sadd(key, value)

    # 处理结构不完整的json数据
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, idea_id, idea_title, idea_describe_1, \
        idea_describe_2, idea_url_visit, idea_url_show, idea_url_visit_moved, idea_url_show_moved, key_word, \
        search_word, cost, cpc, click, match = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'group_id': group_id, 'group': group,
                      'idea_id': idea_id,
                      'idea_title': idea_title, 'idea_describe_1': idea_describe_1, 'idea_describe_2': idea_describe_2,
                      'idea_url_visit': idea_url_visit, 'idea_url_show': idea_url_show,
                      'idea_url_visit_moved': idea_url_visit_moved, 'idea_url_show_moved': idea_url_show_moved,
                      'key_word': key_word, 'search_word': search_word, 'cost': cost, 'cpc': cpc, 'click': click,
                      'match': match}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('daily_search_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    dsr = DailySearch()
    dsr.num_threads = 5
    dsr.save_mode = 3
    dsr.table_name = 'sogou_search_daily_report'
    dsr.data_source_key = 'sogou_accountId_daily_valid_' + start_date.replace('-', '')
    dsr.report_start_date = start_date
    dsr.report_end_date = end_date
    dsr.report_type = '6'
    dsr.unit_of_time = '1'
    dsr.mysql_interface = 'ReportService(DailySearch)'
    dsr.start()
