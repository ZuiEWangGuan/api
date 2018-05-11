#################################
# File Name: DailyIdeaReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class DailyIdea(ReportHandler):

    # 日报表账户层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        date = fields[1]
        idea_id = fields[7]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + ideaId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', ''), account_id, idea_id)
        return row_key

    # 日报表账户层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, idea_id, idea_title, idea_describe_1, \
        idea_describe_2, idea_url_visit, idea_url_show, idea_url_visit_moved, idea_url_show_moved, cost, cpc, \
        click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'group_id': group_id, 'group': group, 'idea_id': idea_id, 'idea_title': idea_title,
                      'idea_describe_1': idea_describe_1, 'idea_describe_2': idea_describe_2,
                      'idea_url_visit': idea_url_visit, 'idea_url_show': idea_url_show,
                      'idea_url_visit_moved': idea_url_visit_moved, 'idea_url_show_moved': idea_url_show_moved,
                      'cost': cost, 'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)

    # 有数据的账户id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        key = 'sogou_api_valid_daily_idea_{}'.format(self.report_start_date.replace('-', ''))
        idea_id = fields[7]
        value = '{}_{}_{}'.format(agent_user_name, account_id, idea_id)
        r.sadd(key, value)

    # 处理结构不完整的json字符串
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, idea_id, idea_title, idea_describe_1, \
        idea_describe_2, idea_url_visit, idea_url_show, idea_url_visit_moved, idea_url_show_moved, cost, cpc, \
        click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'group_id': group_id, 'group': group,
                      'idea_id': idea_id,
                      'idea_title': idea_title, 'idea_describe_1': idea_describe_1, 'idea_describe_2': idea_describe_2,
                      'idea_url_visit': idea_url_visit, 'idea_url_show': idea_url_show,
                      'idea_url_visit_moved': idea_url_visit_moved, 'idea_url_show_moved': idea_url_show_moved,
                      'cost': cost, 'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('daily_idea_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    dir = DailyIdea()
    dir.num_threads = 5
    dir.table_name = 'sogou_idea_daily_report'
    dir.data_source_key = 'sogou_accountId_daily_valid_' + start_date.replace('-', '')
    dir.report_start_date = start_date
    dir.report_end_date = end_date
    dir.report_type = '4'
    dir.unit_of_time = '1'
    dir.mysql_interface = 'ReportService(DailyIdea)'
    dir.start()
