#################################
# File Name: HourlyGroupReport
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-25
#################################

from SoGouReportHandler import *
from loggerConfig import Loggers


class HourlyGroup(ReportHandler):

    @staticmethod
    def row_key_design_single(fields, account_id):
        date = fields[1]
        group_id = fields[5]
        # row_key: id最后一位 + | + yyyyMMdd + accountId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date.replace('-', '').replace(' ', ''), account_id, group_id)
        return row_key

    # 小时报表推广组层级的row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        group_id = fields[0]
        date = fields[1]
        # row_key: id最后一位 + | + yyyyMMdd + accountId + groupId
        row_key = '{}|{}|{}|{}'.format(account_id[-1:], date, account_id, group_id)
        return row_key

    # 小时报表推广组层级的value分析拼接
    @staticmethod
    def value_design(fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id, 'plan': plan,
                      'group_id': group_id, 'group': group, 'cost': cost, 'cpc': cpc, 'click': click,
                      'impression': impression, 'ctr': ctr}
        key_and_value = ('{}_{}'.format(group_id, date.replace('-', '')[:-3]), json.dumps(value_dict))
        return key_and_value

    # 有数据的推广组id存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        r = RedisConn.get_redis_conn()
        date = fields[1]
        key = 'sogou_api_valid_hourly_group_{}'.format(date)
        group_id = fields[0]
        value = '{}_{}_{}'.format(agent_user_name, account_id, group_id)
        r.sadd(key, value)

    # 处理结构不完整的json数据
    def json_overwrite(self, json_str, fields, account_id):
        primary_key, date, user_name, plan_id, plan, group_id, group, cost, cpc, click, impression, ctr = fields
        value_dict = {'date': date, 'user_name': user_name, 'account_id': account_id, 'plan_id': plan_id,
                      'plan': Utils.remove_specific_symbol(plan), 'group_id': group_id, 'group': group, 'cost': cost,
                      'cpc': cpc,
                      'click': click, 'impression': impression, 'ctr': ctr}
        return json.dumps(value_dict)


if __name__ == '__main__':
    log = Loggers()
    log.ini_log('hourly_group_reports_', log.get_local_time())
    start_date, end_date = Utils.get_date_by_input_params(sys.argv[1:])
    hgr = HourlyGroup()
    hgr.num_threads = 5
    hgr.save_mode = 4
    hgr.table_name = 'sogou_group_hourly_report'
    hgr.table_name_single = 'sogou_group_hourly_report_single'
    hgr.data_source_key = 'sogou_accountId_hourly_valid_' + start_date.replace('-', '')
    hgr.report_start_date = start_date
    hgr.report_end_date = end_date
    hgr.report_type = '3'
    hgr.unit_of_time = '4'
    hgr.mysql_interface = 'ReportService(HourlyGroup)'
    hgr.start()
