#################################
# File Name: SoGouReportHandler
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2018-04-19
#################################

import getopt
import gzip
import json
import re
import logging
import os
import sys
import threading
import time
from configparser import ConfigParser
from datetime import timedelta, datetime

import happybase
import requests

from Connections import RedisConn
from Mysql import MysqlHandler
from XmlParser import *

cfp = ConfigParser()
cfp.read('SoGou.ini', encoding='utf-8')

download_local_path = cfp.get('file', 'download_local_path')
hbase_host = cfp.get('hbase', 'hbase_host')

# 报表类型字典
report_type_dict = {'1': 'account', '2': 'plan', '3': 'group', '4': 'idea', '5': 'keyword', '6': 'search'}
# 时间单位字典
unit_of_time_dict = {'1': 'daily', '4': 'hourly'}
# 错误返回码参考字典
response_failure_code = {
    '6': 'Username is invalid',
    '26': 'This IP is forbidden',
    '30': 'your account status is invalid, please contact SoGou custom service',
    '1025005': 'Report performance data error',
    '1025008': 'Request report ReportID is invalid',
    '1025009': 'Request report data is null'
}


class ReportHandler:

    def __init__(self):
        """线程数"""
        self.num_threads = 0
        """hbase表名"""
        self.table_name = ''
        """hbase表名(小时报逐条存储)"""
        self.table_name_single = ''
        """报表起始日期"""
        self.report_start_date = ''
        """报表结束日期"""
        self.report_end_date = ''
        """报表类型"""
        self.report_type = ''
        """统计时间单位"""
        self.unit_of_time = ''
        """接口名(供mysql展示)"""
        self.mysql_interface = ''
        """存储模式 {1: '每一行数据存储一条(默认)', 2: '将多行数据相同key的存储为一条', 3: '将所有数据拼接成一条', 4: '1+2'}"""
        self.save_mode = 1
        """数据源redis的key"""
        self.data_source_key = ''
        """数据源总条数"""
        self.data_source_total = 0
        """分钟请求上限限制"""
        self.request_minute_limit = 1500
        """分钟请求数"""
        self.request_minute_count = 0
        """分钟请求开始时间"""
        self.request_minute_start_time = time.time()
        """起始时间"""
        self.start_time = datetime.now()
        """起始时间(time)"""
        self.start_time_mysql = time.time()
        """获取redis客户端"""
        # self.r = RedisConn.get_redis_conn()
        """获取hbase连接池"""
        self.pool = happybase.ConnectionPool(size=3, host=hbase_host)
        """获取mysql客户端"""
        self.mysql_conn = MysqlHandler.get_mysql_conn()
        """线程锁"""
        self.locker = threading.Lock()
        """初始化角标"""
        self.it_count = 0
        """SoGou API url"""
        self.url = 'http://api.agent.sogou.com:80'
        """SoGou API SOAP service name"""
        self.service_name = 'ReportService'
        """缓存数据(tuple(row_key, value))"""
        self.hbase_list = []
        """缓存数据(tuple(row_key, value)) 小时报逐条缓存"""
        self.hbase_list_single = []
        """请求总次数"""
        self.request_count = 0
        """请求轮数"""
        self.request_cycle_count = 0
        """每轮成功次数"""
        self.request_success_count = 0
        """每轮失败次数"""
        self.request_failure_count = 0
        """入库条数"""
        self.data_count = 0
        """入库条数(逐条存储)"""
        self.data_single_count = 0
        """修正成功数据统计"""
        self.redefined_succeed_count = 0
        """修正失败数据统计"""
        self.redefined_failed_count = 0

    # 整体流程控制
    def start(self):
        """启动日志"""
        self.mysql_start_info(account_key='user', start_time=self.start_time_mysql, media_name='SoGou')
        """启动多线程执行任务"""
        self.powered_by_threads()
        """结束日志"""
        self.mysql_end_info(account_total=-1, data_total=self.data_count, request_total=self.request_count,
                            media_name='SoGou')

    # 使用多线程请求接口
    def powered_by_threads(self):
        """创建文件夹"""
        self.make_directory()
        """获取源数据"""
        agid_auser_apasswd_token_user_account = self.get_auser_apasswd_token_user_account()
        threads = []
        logging.info('source data counts: ' + str(len(agid_auser_apasswd_token_user_account)))
        """赋值"""
        self.data_source_total = len(agid_auser_apasswd_token_user_account)
        """创建线程"""
        for i in range(self.num_threads):
            t = threading.Thread(target=self.cycle_for_get_reports, args=(agid_auser_apasswd_token_user_account,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        """将缓存中剩余的数据插入hbase"""
        self.send_to_hbase()
        self.send_to_hbase_single()
        end_time = datetime.now()
        logging.info('run time: {}'.format(end_time - self.start_time))
        logging.info('request total: {}, request cycle total: {}, cycle success: {}, cycle failure: {}, '
                     'redefined succeed:{}, redefined failed:{}, data_count: {}, data_single_count: {}'
                     .format(self.request_count, self.request_cycle_count, self.request_success_count,
                             self.request_failure_count, self.redefined_succeed_count, self.redefined_failed_count,
                             self.data_count, self.data_single_count))
        logging.info('FINISHED')

    # 从数据源list中排队读取数据并开始执行
    def cycle_for_get_reports(self, agid_auser_apasswd_token_user_account):
        list_len = len(agid_auser_apasswd_token_user_account)
        while True:
            """线程锁, 获取list角标并递增"""
            with self.locker:
                list_index = self.it_count
                self.it_count += 1
            if list_index >= list_len:
                break
            else:
                logging.info('============================= request rate: {}% ============================='.format(
                    round(list_index / self.data_source_total * 100, 2)))
                """开始请求接口"""
                agency_id, agent_user, agent_password, token, user_name, account_id = \
                    agid_auser_apasswd_token_user_account[list_index]
                self.get_report(agency_id=agency_id, agent_user_name=agent_user, agent_password=agent_password,
                                token=token, user_name=user_name, account_id=account_id)

    # 流程控制
    def get_report(self, agency_id, agent_user_name, agent_password, token, user_name, account_id):
        """请求轮数+1"""
        self.request_cycle_count_add(1)
        report_level = report_type_dict.setdefault(self.report_type, 'others')
        time_level = unit_of_time_dict.setdefault(self.unit_of_time, 'others')
        """获取默认参数"""
        default_request = self.get_default_request(agent_user_name=agent_user_name, agent_password=agent_password,
                                                   token=token, user_name=user_name)
        """获取报表Id"""
        report_id = self.get_report_id(default_request=default_request, account_id=account_id, user_name=user_name)
        """若报表id为failure, 则退出后续操作"""
        if report_id == 'failure':
            return
        """默认重试次数为0"""
        retry_count = 0
        """请求get/state接口的开始时间"""
        start_time = datetime.now()
        while True:
            """获取报表状态"""
            state = self.get_report_state(default_request=default_request, account_id=account_id, user_name=user_name,
                                          report_id=report_id)
            if state == 'failure':
                return
            if state == '1':
                logging.info('get status used time: {}'.format(datetime.now() - start_time))
                break
            retry_count += 1
            logging.info('user: {}, retry to get status for {} times'.format(user_name, retry_count))
            time.sleep(0.5)
        """获取下载url"""
        url = self.get_report_path(default_request=default_request, user_name=user_name, report_id=report_id,
                                   account_id=account_id)
        """若下载地址为failure, 则退出后续操作"""
        if url == 'failure':
            return
        """下载文件到本地"""
        file_path = self.file_download(user_name=user_name, account_id=account_id, report_url=url,
                                       report_level=report_level, time_level=time_level)
        """解析gzip文件"""
        csv_lines = self.parse_gzip_file(file_path=file_path, user_name=user_name)
        """设计row_key, 拼接value, 并存入hbase中"""
        if self.save_mode == 1:
            self.save_reports(csv_lines=csv_lines, agency_id=agency_id, user_name=user_name, account_id=account_id,
                              agent_user_name=agent_user_name)
        elif self.save_mode == 2:
            self.save_reports_joined(csv_lines=csv_lines, agency_id=agency_id, user_name=user_name,
                                     account_id=account_id, agent_user_name=agent_user_name)
        elif self.save_mode == 3:
            self.save_reports_joined_all(csv_lines=csv_lines, agency_id=agency_id, user_name=user_name,
                                         account_id=account_id, agent_user_name=agent_user_name)
        elif self.save_mode == 4:
            self.save_reports_once_and_joined(csv_lines=csv_lines, agency_id=agency_id, user_name=user_name,
                                              account_id=account_id, agent_user_name=agent_user_name)
        else:
            logging.error('missed for saving report')

    # 获取报表Id
    def get_report_id(self, default_request, account_id, user_name):
        while True:
            """拼接请求信息"""
            report_id_request = self.get_report_id_request(default_request=default_request)
            """请求接口, 获取response"""
            report_id_response = self.request_post(report_id_request)
            """解析xml, 转换为json格式"""
            report_id_dict = XMLParser().parse(report_id_response.text)
            """获取返回状态"""
            status = report_id_dict['ns3:desc']
            """若为success则取值reportId, 否则为reportId赋值为failure"""
            if status == 'success':
                report_id = report_id_dict['reportId']
                logging.info('account: {}, user {} get reportId {} succeed'.format(account_id, user_name, report_id))
                break
            else:
                message = report_id_dict['ns3:message']
                code = report_id_dict['ns3:code']
                logging.info(
                    'account: {}, user {} get report id failure, code: {}, message: {}, so, passed.'.format(account_id,
                                                                                                            user_name,
                                                                                                            code,
                                                                                                            message))
                print(str(report_id_dict))
                report_id = 'failure'
                """失败次数+1"""
                self.request_failure_count_add(1)
                if code != '1025009':
                    """记录错误日志"""
                    self.mysql_record_error_log(date=self.report_start_date, account=account_id, code=code,
                                                media_name='SoGou',
                                                error_msg='site: {}, message: {}'.format('get id', message))
                if code != '26':
                    break
                else:
                    time.sleep(30)
                    logging.warning('This IP is forbidden, wait for 30 seconds')
        return report_id

    # 获取报表状态
    def get_report_state(self, account_id, user_name, default_request, report_id):
        """拼接请求信息"""
        report_state_request = self.get_report_state_request(default_request, report_id)
        """请求接口, 获取response"""
        report_state_response = self.request_post(report_state_request)
        """解析xml, 转换为json格式"""
        report_state_dict = XMLParser().parse(report_state_response.text)
        """获取返回状态"""
        status = report_state_dict['ns3:desc']
        """若为success则获取生成状态码, 否则赋值failure"""
        if status == 'success':
            is_generated = report_state_dict['isGenerated']
            logging.info('account: {}, user {} get state succeed, is_generated: {}'.format(account_id, user_name,
                                                                                           is_generated))
        else:
            code = report_state_dict['ns3:code']
            message = report_state_dict['ns3:message']
            if code == '1025008':
                is_generated = '0'
            elif code == '26':
                logging.warning('This IP is forbidden, wait for 30 seconds')
                is_generated = '0'
                time.sleep(30)
            else:
                is_generated = 'failure'
                """失败次数+1"""
                self.request_failure_count_add(1)
                logging.info(
                    'account: {}, user {} get report status failure, code: {}, message: {}, so, passed.'.format(
                        account_id, user_name, code, message))
            if code != '1025009':
                self.mysql_record_error_log(date=Utils.get_today_by_point(), account=account_id,
                                            code=code, media_name='SoGou',
                                            error_msg='site: {}, message: {}'.format('get state', message))
            # if code == '26':
            #     logging.warning('This IP is forbidden, wait for 30 seconds')
            #     time.sleep(30)
        return is_generated

    # 获取报表下载路径
    def get_report_path(self, default_request, user_name, report_id, account_id):
        while True:
            """拼接请求信息"""
            report_path_request = self.get_report_path_request(default_request, report_id)
            """请求接口, 获取response"""
            report_path_response = self.request_post(report_path_request)
            """解析xml, 转换为json格式"""
            report_path_dict = XMLParser().parse(report_path_response.text)
            """若为success则获取报表下载地址, 否则赋值failure"""
            if 'success' == report_path_dict['ns3:desc']:
                report_path = report_path_dict['reportFilePath']
                logging.info(
                    'account: {}, user {} get report path succeed, report path: {}'.format(account_id, user_name,
                                                                                           report_path))
                break
            else:
                code = report_path_dict['ns3:code']
                message = report_path_dict['ns3:message']
                logging.info(
                    'account: {}, user {} get report path failure, code: {}, message: {}'.format(account_id, user_name,
                                                                                                 code, message))
                report_path = 'failure'
                """失败次数+1"""
                self.request_failure_count_add(1)
                if code != '1025009':
                    """记录错误日志"""
                    self.mysql_record_error_log(date=Utils.get_today_by_point(), account=account_id, code=code,
                                                media_name='SoGou',
                                                error_msg='site: {}, message: {}'.format('get path', message))
                if code != '26':
                    break
                else:
                    time.sleep(30)
                    logging.warning('This IP is forbidden, wait for 30 seconds')
        return report_path

    # 下载报表
    @staticmethod
    def file_download(user_name, account_id, report_url, report_level, time_level):
        """下载文件"""
        get = requests.get(report_url)
        """本地存储路径"""
        # eg: /home/api/SoGou/dixon/SoGouDownload/20180422/daily_account/hancheng145@sina.cn.gz
        file_path = "{}/{}/{}_{}/{}.gz".format(download_local_path, Utils.get_today(), time_level,
                                               report_level, user_name)
        """文件写入本地磁盘"""
        with open(file_path, "wb") as code:
            code.write(get.content)
            logging.info('account: {}, user: {} report download finished'.format(account_id, user_name))
        return file_path

    # 解析文件
    @staticmethod
    def parse_gzip_file(file_path, user_name):
        with gzip.open(filename=file_path) as f:
            """舍弃前两行无用数据"""
            lines = f.readlines()[2:]
            logging.info('user: {} gzip file parse and read succeed'.format(user_name))
        #############################################
        # for line in lines:
        #     logging.info(line.decode('gbk')[:-1])
        #############################################
        return lines

    # 保存数据(存储模式: 每一行数据存储一条)
    def save_reports(self, csv_lines, agency_id, user_name, account_id, agent_user_name):
        for csv_line_b in csv_lines:
            """字节 -> 字符串(舍弃行尾换行符)"""
            csv_line = csv_line_b.decode('gbk')[:-1]
            """以逗号分隔符切分字符串, 得到元素list"""
            fields = csv_line.split(',')
            """row_key设计, 需要继承并重写函数"""
            row_key = self.row_key_design(fields=fields, account_id=account_id)
            """value分析拼接设计, 需要继承并重写函数"""
            json_str = self.value_design(fields=fields, account_id=account_id)
            """检查json正确性"""
            value = self.json_check(json_str=json_str, fields=fields, account_id=account_id)
            # print(value)
            if value == 'failure':
                return
            """存入缓存list中(缓存大于5000条自动发送)"""
            self.send_to_list(row_key=row_key, value=value)
            """存入redis"""
            self.save_in_redis(agency_id=agency_id, fields=fields, user_name=user_name, account_id=account_id,
                               agent_user_name=agent_user_name)
        logging.info('account: {}, user: {} send to list succeed'.format(account_id, user_name))
        """成功次数+1"""
        self.request_success_count_add(1)

    # 保存数据(存储模式: 将多行数据相同key的存储为一条)
    def save_reports_joined(self, csv_lines, agency_id, user_name, account_id, agent_user_name):
        temp_dict = {}
        for csv_line_b in csv_lines:
            """字节 -> 字符串(舍弃行尾换行符)"""
            csv_line = csv_line_b.decode('gbk')[:-1]
            """以逗号分隔符切分字符串, 得到元素list"""
            fields = csv_line.split(',')
            """key, value分析, 需要继承并重写函数"""
            key, json_str = self.value_design(fields=fields, account_id=account_id)
            """检查json正确性"""
            value = self.json_check(json_str=json_str, fields=fields, account_id=account_id)
            if value == 'failure':
                return
            """相同key的拼接到缓存dict中"""
            old_value = temp_dict.setdefault(key, '')
            """将同key的数据以逗号分隔符拼接到缓存dict中"""
            temp_dict.update({key: old_value + value + ','})
        self.put_data_from_dict_to_list(temp_dict=temp_dict, agency_id=agency_id, user_name=user_name,
                                        account_id=account_id, agent_user_name=agent_user_name)
        logging.info('account: {}, user: {} send to list succeed'.format(account_id, user_name))
        """成功次数+1"""
        self.request_success_count_add(1)

    # 保存数据(存储模式: 将多行数据拼接成一条存储)
    def save_reports_joined_all(self, csv_lines, agency_id, user_name, account_id, agent_user_name):
        fields = []
        temp_str = ''
        for csv_line_b in csv_lines:
            """字节 -> 字符串(舍弃行尾换行符)"""
            csv_line = csv_line_b.decode('gbk')[:-1]
            """以逗号分隔符切分字符串, 得到元素list"""
            fields = csv_line.split(',')
            """value分析拼接设计, 需要继承并重写函数"""
            json_str = self.value_design(fields=fields, account_id=account_id)
            """检查json正确性"""
            value = self.json_check(json_str=json_str, fields=fields, account_id=account_id)
            if value == 'failure':
                return
            """json格式的value拼接到缓存中"""
            temp_str += value + ','
        """整理数据格式"""
        res_value = '[' + temp_str[:-1] + ']'
        """获取row_key"""
        row_key = self.row_key_design(fields=fields, account_id=account_id)
        """存入缓存list中(缓存大于5000条自动发送)"""
        self.send_to_list(row_key=row_key, value=res_value)
        """存入redis"""
        self.save_in_redis(fields=fields, agency_id=agency_id, user_name=user_name, account_id=account_id,
                           agent_user_name=agent_user_name)
        logging.info('account: {}, user: {} send to list succeed'.format(account_id, user_name))
        """成功次数+1"""
        self.request_success_count_add(1)

    # 保存数据(存储模式: 1+2)
    def save_reports_once_and_joined(self, csv_lines, agency_id, user_name, account_id, agent_user_name):
        temp_dict = {}
        for csv_line_b in csv_lines:
            """字节 -> 字符串(舍弃行尾换行符)"""
            csv_line = csv_line_b.decode('gbk')[:-1]
            """以逗号分隔符切分字符串, 得到元素list"""
            fields = csv_line.split(',')
            """row_key设计(每条分别存储)"""
            row_key_single = self.row_key_design_single(fields=fields, account_id=account_id)
            """key, value分析, 需要继承并重写函数"""
            key, json_str = self.value_design(fields=fields, account_id=account_id)
            """检查json正确性"""
            value = self.json_check(json_str=json_str, fields=fields, account_id=account_id)
            if value == 'failure':
                return
            """单条存储"""
            self.send_to_list_single(row_key=row_key_single, value=value)
            """相同key的拼接到缓存dict中"""
            old_value = temp_dict.setdefault(key, '')
            """将同key的数据以逗号分隔符拼接到缓存dict中"""
            temp_dict.update({key: old_value + value + ','})
        self.put_data_from_dict_to_list(temp_dict=temp_dict, agency_id=agency_id, user_name=user_name,
                                        account_id=account_id, agent_user_name=agent_user_name)
        logging.info('account: {}, user: {} send to list succeed'.format(account_id, user_name))
        """成功次数+1"""
        self.request_success_count_add(1)

    # 存入缓存list中
    def send_to_list(self, row_key, value):
        logging.info('table_name: {}, row_key: {}'.format(self.table_name, row_key))
        # logging.debug('table_name: {}, row_key: {}, value: {}'.format(self.table_name, row_key, value))
        """线程锁"""
        with self.locker:
            """元组数据"""
            tuple_data = (row_key, value)
            """插入list缓存中"""
            self.hbase_list.append(tuple_data)
            """入库条数统计+1"""
            self.data_count_add(1)
            """若缓存数大于5000, 则批量发送到hbase数据库"""
            if len(self.hbase_list) >= 5000:
                self.send_to_hbase()

    # 存入single list中
    def send_to_list_single(self, row_key, value):
        logging.info('table_name: {}, row_key: {}'.format(self.table_name_single, row_key))
        # logging.debug('table_name: {}, row_key: {}, value: {}'.format(self.table_name, row_key, value))
        """线程锁"""
        with self.locker:
            """元组数据"""
            tuple_data = (row_key, value)
            """插入list缓存中"""
            self.hbase_list_single.append(tuple_data)
            """入库条数统计+1"""
            self.data_single_count_add(1)
            """若缓存数大于5000, 则批量发送到hbase数据库"""
            if len(self.hbase_list_single) >= 5000:
                self.send_to_hbase_single()

    # 存入hbase
    def send_to_hbase(self):
        logging.info('sending to hbase ...')
        """获取hbase连接"""
        with self.pool.connection() as connection:
            """指定表"""
            table = connection.table(self.table_name)
            """获得批量发送的对象"""
            batch = table.batch()
            with batch as bat:
                for row_key, value in self.hbase_list:
                    """插入批量发送对象中(退出上下文管理器自动发送)"""
                    bat.put(row=row_key, data={'info:data': value})
        logging.info('save in hbase succeed, table name: {}'.format(self.table_name))
        """清楚缓存表"""
        self.hbase_list.clear()
        logging.info('local list cleared finished')

    # 存入hbase single
    def send_to_hbase_single(self):
        logging.info('sending to hbase ...')
        """获取hbase连接"""
        with self.pool.connection() as connection:
            """指定表"""
            table = connection.table(self.table_name_single)
            """获得批量发送的对象"""
            batch = table.batch()
            with batch as bat:
                for row_key, value in self.hbase_list_single:
                    """插入批量发送对象中(退出上下文管理器自动发送)"""
                    bat.put(row=row_key, data={'info:data': value})
        logging.info('save in hbase succeed, table name: {}'.format(self.table_name_single))
        """清楚缓存表"""
        self.hbase_list_single.clear()
        logging.info('local list cleared finished')

    # 存入redis
    def save_in_redis(self, fields, agency_id, user_name, account_id, agent_user_name):
        """this function need to be overwrite"""
        logging.error('redis was not insert')

    # 将缓存dict中的数据遍历插入缓存list中
    def put_data_from_dict_to_list(self, temp_dict, agency_id, user_name, account_id, agent_user_name):
        for key_and_date, data_set in temp_dict.items():
            fields = key_and_date.split('_')
            """拼接value"""
            value = '[' + data_set[:-1] + ']'
            """row_key设计"""
            row_key = self.row_key_design(fields=fields, account_id=account_id)
            """存入缓存list中"""
            self.send_to_list(row_key=row_key, value=value)
            """存入redis"""
            self.save_in_redis(fields=fields, agency_id=agency_id, user_name=user_name, account_id=account_id,
                               agent_user_name=agent_user_name)

    # http/post请求
    def request_post(self, request_body):
        wsdl = self.get_wsdl()
        """总请求次数+1"""
        self.request_count_add(1)
        """分钟请求次数限制"""
        self.request_count_control()
        response = requests.post(wsdl, data=request_body.encode('utf-8'))
        return response

    # wsdl url
    def get_wsdl(self):
        wsdl = self.url + '/sem/sms/v1/' + self.service_name + '?wsdl'
        return wsdl

    # 默认参数
    @staticmethod
    def get_default_request(agent_user_name, agent_password, token, user_name):
        default_request = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
        xmlns:v1="http://api.sogou.com/sem/common/v1" xmlns:v11="https://api.sogou.com/sem/sms/v1">
                        <soapenv:Header>
                        <v1:AuthHeader>
                        <v1:agentusername>{}</v1:agentusername>
                        <v1:agentpassword>{}</v1:agentpassword>
                        <v1:username>{}</v1:username>
                        <!-- <v1:password></v1:password> -->
                        <v1:token>{}</v1:token>
                        </v1:AuthHeader>
                        </soapenv:Header>'''.format(agent_user_name, agent_password, user_name, token) + '''
                        <soapenv:Body>
                        {}
                        </soapenv:Body>
                        </soapenv:Envelope>'''
        return default_request

    # 拉取reportId的请求体
    def get_report_id_request(self, default_request):
        request_body = '''<v11:getReportId>
        <reportRequestType>
        <performanceData>cost</performanceData>
        <performanceData>cpc</performanceData>
        <performanceData>click</performanceData>
        <performanceData>impression</performanceData>
        <performanceData>ctr</performanceData>
        <startDate>{}T00:00:00.000</startDate>
        <endDate>{}T23:59:59.999</endDate>
        <reportType>{}</reportType>
        <unitOfTime>{}</unitOfTime>
        </reportRequestType>
        </v11:getReportId>'''.format(self.report_start_date, self.report_end_date, self.report_type, self.unit_of_time)
        request = default_request.format(request_body)
        return request

    # 拉取reportState的请求体
    @staticmethod
    def get_report_state_request(default_request, report_id):
        request_body = '''<v11:getReportState>
                <reportId>{}</reportId>
                </v11:getReportState>'''.format(report_id)
        request = default_request.format(request_body)
        return request

    # 拉取reportPath的请求体
    @staticmethod
    def get_report_path_request(default_request, report_id):
        request_body = '''<v11:getReportPathRequest>
                        <reportId>{}</reportId>
                        </v11:getReportPathRequest>'''.format(report_id)
        request = default_request.format(request_body)
        return request

    # 获取数据源(代理商id, 代理商密码, token, userName)
    def get_auser_apasswd_token_user_account(self):
        r = RedisConn.get_redis_conn()
        logging.info('get accounts from key: {}'.format(self.data_source_key))
        agid_aguser_agpasswd_token_user_account_list = []
        agid_and_token = r.hgetall('sogou_access_token')
        for k, v in agid_and_token.items():
            agency_id = k.decode("utf-8")
            access_token = v.decode("utf-8")
            aguser_agpasswd = r.hget('sogou_agencysname', agency_id)
            field2 = aguser_agpasswd.decode('utf-8').split('_')
            agent_user_name = field2[0]
            agent_user_password = field2[1]
            username_accountid = r.zrangebyscore(self.data_source_key, agency_id, agency_id)
            for user_account_b in username_accountid:
                user_account_list = user_account_b.decode('utf-8').split('##|')
                user_name = user_account_list[0]
                account_id = user_account_list[1]
                tup = (agency_id, agent_user_name, agent_user_password, access_token, user_name, account_id)
                agid_aguser_agpasswd_token_user_account_list.append(tup)
        return agid_aguser_agpasswd_token_user_account_list

    # 创建文件夹
    def make_directory(self):
        time_level = unit_of_time_dict.setdefault(self.unit_of_time, 'others')
        report_level = report_type_dict.setdefault(self.report_type, 'others')
        date = Utils.get_today()
        directory_name = "{}/{}/{}_{}".format(download_local_path, date, time_level,
                                              report_level)
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)
            logging.info('make directory "{}" succeed'.format(directory_name))
        else:
            logging.info('directory "{}" had been created'.format(directory_name))

    # row_key设计
    @staticmethod
    def row_key_design(fields, account_id):
        """this function need to be overwrite"""
        logging.error('row_key was not designed')
        return 'failure'

    # row_key设计
    @staticmethod
    def row_key_design_single(fields, account_id):
        """this function need to be overwrite"""
        logging.error('row_key was not designed')
        return 'failure'

    # 分析数据, 拼接value
    @staticmethod
    def value_design(fields, account_id):
        """this function need to be overwrite"""
        logging.error('value was not designed')
        return 'failure'

    # 反解json检查
    def json_check(self, json_str, fields, account_id):
        try:
            json.loads(json_str)
            return json_str
        except json.decoder.JSONDecodeError:
            """修正json数据"""
            checked = self.json_overwrite(json_str=json_str, fields=fields, account_id=account_id)
            try:
                json.loads(checked)
                logging.info("redefined success! error json: '{}', correction json: '{}'".format(json_str, checked))
                """修正成功次数+1"""
                self.redefined_succeed_count_add()
                return checked
            except json.decoder.JSONDecodeError:
                """记录错误日志"""
                self.mysql_record_error_log(date=self.report_start_date, account=account_id, code=-1,
                                            media_name='SoGou', error_msg='error json: {}'.format(json_str))
                logging.error("json: '{}', needs to be redefined.".format(json_str))
                """修正失败数+1"""
                self.redefined_failed_count_add()
                return 'failure'

    # 修正json数据
    def json_overwrite(self, json_str, fields, account_id):
        """记录错误日志"""
        self.mysql_record_error_log(date=self.report_start_date, account=account_id, code=-1,
                                    media_name='SoGou',
                                    error_msg='error json: {}'.format(json_str))
        logging.error("json: '{}', needs to be redefined")

    # 请求从次数递增(线程安全)
    def request_count_add(self, count):
        with self.locker:
            self.request_count += count

    # 请求轮数递增(线程安全)
    def request_cycle_count_add(self, count):
        with self.locker:
            self.request_cycle_count += count

    # 每轮成功次数递增(线程安全)
    def request_success_count_add(self, count):
        with self.locker:
            self.request_success_count += count

    # 每轮失败次数递增(线程安全)
    def request_failure_count_add(self, count):
        with self.locker:
            self.request_failure_count += count

    # 修正成功数据统计递增(线程安全)
    def redefined_succeed_count_add(self):
        with self.locker:
            self.redefined_succeed_count += 1

    # 修正失败数据统计递增(线程安全)
    def redefined_failed_count_add(self):
        with self.locker:
            self.redefined_failed_count += 1

    # 每分钟请求次数限制(伪)
    def request_count_control(self):
        with self.locker:
            self.request_minute_count += 1
            # print('分钟请求数: ', self.request_minute_count)
            now = time.time()
            time_passed = now - self.request_minute_start_time
            if time_passed > 60:
                logging.info('1 minute reached, request total: {}'.format(self.request_minute_count))
                self.request_minute_start_time = now
                self.request_minute_count = 0
            if self.request_minute_count >= self.request_minute_limit:
                logging.info(
                    'minutes control reached, request for {} times, reach {}, use {} seconds, '
                    'and wait for 10 seconds ...'.format(
                        self.request_minute_count, self.request_minute_limit, int(time_passed)))
                time.sleep(10)

    # 入库条数
    def data_count_add(self, count):
        self.data_count += count

    # 入库条数(逐条存储)
    def data_single_count_add(self, count):
        self.data_single_count += count

    # 启动日志
    def mysql_start_info(self, account_key, start_time, media_name):
        """记录API结果日志信息"""
        process_param = '--date={0} --account_key={1} --url={2}'.format(self.report_start_date, account_key,
                                                                        self.mysql_interface)
        MysqlHandler.put_api_result_info(self.mysql_conn, self.report_start_date, self.mysql_interface, start_time, 0,
                                         media_name, 0, 0, process_param)

    # 结束日志
    def mysql_end_info(self, account_total, data_total, request_total, media_name):
        end_time = time.time()
        logging.info("end info: [run_time] {} [account_total] {} [data_total] {}".format(
            end_time - self.start_time_mysql,
            account_total,
            data_total,
        ))

        """记录API结果日志信息"""
        process_param = '--date={0} --account_key={1} --url={2}'.format(self.report_start_date,
                                                                        report_type_dict.setdefault(self.report_type,
                                                                                                    'others'),
                                                                        self.mysql_interface)
        MysqlHandler.put_api_result_info(self.mysql_conn, self.report_start_date, self.mysql_interface,
                                         self.start_time_mysql, end_time, media_name, account_total, data_total,
                                         process_param)

        """记录API统计日志信息"""
        MysqlHandler.put_api_statistics_info(self.mysql_conn, self.mysql_interface, request_total)

        MysqlHandler.close_mysql_conn(self.mysql_conn)

    # 记录错误日志
    def mysql_record_error_log(self, date, account, code, media_name, error_msg):
        """将错误信息记录到mysql"""
        mysql = MysqlHandler()
        mysql.put_api_error_info(mysql.get_mysql_conn(), date, account, self.mysql_interface, media_name, error_msg,
                                 code)


class Utils:

    # 获取前一天日期, 以 - 为分隔符
    @staticmethod
    def get_yesterday_by_point():
        yesterday = datetime.today() + timedelta(-1)
        return yesterday.strftime("%Y-%m-%d")

    # 获取前一天日期
    @staticmethod
    def get_yesterday():
        yesterday = datetime.today() + timedelta(-1)
        return yesterday.strftime("%Y%m%d")

    # 获取当天日期
    @staticmethod
    def get_today():
        today = datetime.today()
        return today.strftime("%Y%m%d")

    # 获取当天日期
    @staticmethod
    def get_today_by_point():
        today = datetime.today()
        return today.strftime("%Y-%m-%d")

    # 转换编码
    @staticmethod
    def bytes_to_str(byte):
        return byte.decode('utf-8')

    # 解析外部传入参数，未传入则查前一天，传入则查指定日期范围，异常则提示并退出
    @staticmethod
    def get_date_by_input_params(argv):
        if len(argv) > 0:
            report_start_date = ''
            report_end_date = ''
            tag_words = 'SoGouReportHandlerInputDates -s <hourly report start date: "yyyy-MM-dd">' \
                        ' -e <hourly report end date: "yyyy-MM-dd">'
            try:
                opts, args = getopt.getopt(argv, "hs:e:", ["start_date=", "end_date="])
            except getopt.GetoptError:
                sys.exit()
            for opt, arg in opts:
                if opt == '-h':
                    logging.info(tag_words)
                    sys.exit()
                elif opt in ("-s", "--start_date"):
                    report_start_date = arg
                elif opt in ("-e", "--end_date"):
                    report_end_date = arg
                else:
                    logging.info(tag_words)
                    sys.exit()
            logging.info('report start date: {}, report end date: {}'.format(report_start_date, report_end_date))
            date_range = (report_start_date, report_end_date)
            return date_range
        else:
            report_date = Utils.get_yesterday_by_point()
            logging.info('report start date: {}, report end date: {}'.format(report_date, report_date))
            date_range = (report_date, report_date)
            return date_range

    # 保留大小写英文、数字、汉字以及单双引号
    @staticmethod
    def remove_specific_symbol(string):
        rule = re.compile(u"[^a-zA-Z0-9\u4e00-\u9fa5'\"\- ]")
        string = rule.sub('', string)
        string = string.replace('"', '\\"')
        return string

    # 将传入的redis key设置一个月失效时长
    @staticmethod
    def set_lose_efficacy(key_list):
        r = RedisConn.get_redis_conn()
        for key in key_list:
            r.expire(key, 2592000)
            logging.info('redis key: {}, expire success! efficacy: 1 month')
