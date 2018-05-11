#################################
# File Name: DailyPlanReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-24
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class DailyPlan(ReportHandler):

    # 日计划账户层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        date = fields[1]
        plan_id = fields[3]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + planId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', ''), account_id, plan_id)
        return row_key

    # 日计划账户层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'cost': cost, 'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)

    # 有数据的计划id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        key = 'sogou_api_valid_daily_plan_{}'.format(self.report_start_date.replace('-', ''))
        plan_id = fields[3]
        value = '{}_{}_{}'.format(agent_user_name, account_id, plan_id)
        r.sadd(key, value)

    # 处理结构不完整的json字符串
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'cost': cost, 'cpc': cpc, 'click': click, 'impression': impression,
                      'ctr': ctr}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('daily_plan_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    dpr = DailyPlan()
    dpr.num_threads = 5
    dpr.table_name = 'sogou_plan_daily_report'
    dpr.data_source_key = 'sogou_accountId_daily_valid_' + start_date.replace('-', '')
    dpr.report_start_date = start_date
    dpr.report_end_date = end_date
    dpr.report_type = '2'
    dpr.unit_of_time = '1'
    dpr.mysql_interface = 'ReportService(DailyCampaign)'
    dpr.start()
