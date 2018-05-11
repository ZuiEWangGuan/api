#################################
# File Name: DailyGroupReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class DailyGroup(ReportHandler):

    # 日报表账户层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        date = fields[1]
        group_id = fields[5]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + groupId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', ''), account_id, group_id)
        return row_key

    # 日报表账户层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'group_id': group_id, 'group': group, 'cost': cost, 'cpc': cpc, 'click': click,
                      'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)

    # 有数据的账户id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        date = fields[1]
        group_id = fields[5]
        key = 'sogou_api_valid_daily_group_{}'.format(date.replace('-', ''))
        value = '{}_{}_{}'.format(agent_user_name, account_id, group_id)
        r.sadd(key, value)

    # 处理结构不完整的json字符串
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'group_id': group_id, 'group': group, 'cost': cost,
                      'cpc': cpc, 'click': click, 'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('daily_group_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    dgr = DailyGroup()
    dgr.num_threads = 5
    dgr.table_name = 'sogou_group_daily_report'
    dgr.data_source_key = 'sogou_accountId_daily_valid_' + start_date.replace('-', '')
    dgr.report_start_date = start_date
    dgr.report_end_date = end_date
    dgr.report_type = '3'
    dgr.unit_of_time = '1'
    dgr.mysql_interface = 'ReportService(DailyGroup)'
    dgr.start()
