#################################
# File Name: HourlyPlanReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class HourlyPlan(ReportHandler):

    @staticmethod
    def row_key_design_single(fields, account_id):
        date = fields[1]
        plan_id = fields[3]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + plan_id
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', '').replace(' ', ''), account_id, plan_id)
        return row_key

    # 小时报表计划层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        plan_id = fields[0]
        date = fields[1]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + planId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date, account_id, plan_id)
        return row_key

    # 小时报表计划层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'cost': cost, 'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr}
        key_and_value = ('{}_{}'.format(plan_id, date.replace('-', '')[:-3]), json.dumps(value_dict))
        return key_and_value

    # 有数据的计划id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        date = fields[1]
        plan_id = fields[0]
        key = 'sogou_api_valid_hourly_plan_{}'.format(date)
        value = '{}_{}_{}'.format(agent_user_name, account_id, plan_id)
        r.sadd(key, value)

    # 处理结构不完整的json数据
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'cost': cost, 'cpc': cpc, 'click': click,
                      'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('hourly_plan_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    hpr = HourlyPlan()
    hpr.num_threads = 5
    hpr.save_mode = 4
    hpr.table_name = 'sogou_plan_hourly_report'
    hpr.table_name_single = 'sogou_plan_hourly_report_single'
    hpr.data_source_key = 'sogou_accountId_hourly_valid_' + start_date.replace('-', '')
    hpr.report_start_date = Utils.get_yesterday_by_point()
    hpr.report_end_date = Utils.get_yesterday_by_point()
    hpr.report_type = '2'
    hpr.unit_of_time = '4'
    hpr.mysql_interface = 'ReportService(HourlyPlan)'
    hpr.start()
