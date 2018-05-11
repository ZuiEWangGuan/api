# -*- coding: utf-8 -*-

import logging
import os
import time
from configparser import ConfigParser
from datetime import datetime

import pymysql


class MysqlHandler:

    @staticmethod
    def get_mysql_conn():
        try:
            cfp = ConfigParser()
            cfp.read('SoGou.ini', encoding='utf-8')
            mysql_host = cfp.get('mysql', 'mysql_host')
            mysql_user = cfp.get('mysql', 'mysql_user')
            mysql_password = cfp.get('mysql', 'mysql_password')

            # 打开数据库连接
            db = pymysql.connect(mysql_host, mysql_user, mysql_password, "dmp_api")
            logging.debug('mysql connection is successful')
            return db
        except Exception as e:
            logging.error("mysql connection exception!")
            logging.error(repr(e))

    @staticmethod
    def put_data(db, sql):
        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        try:
            # 执行sql语句
            cursor.execute(sql)
            cursor.close()
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()

    @staticmethod
    def put_api_statistics_info(db, url, requests):

        local_times = time.localtime(time.time())
        local_time_format = time.strftime("%Y-%m-%d %H:%M:%S", local_times)

        local_date_format = time.strftime("%Y-%m-%d", local_times)

        """记录API统计日志信息"""
        sql = "insert into api_statistics_log (interface,ymd,sum,create_time) values ('{0}','{1}','{2}','{3}')".format(
            url,
            local_date_format,
            requests,
            local_time_format
        )

        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        try:
            # 执行sql语句
            cursor.execute(sql)
            cursor.close()
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()

    @staticmethod
    def put_api_result_info(db, date, url, st_time, et_time, media_name, account_total, data_total,
                            process_param):
        # data_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        data_date = date

        st_times = time.localtime(st_time)
        st_format_time = time.strftime("%Y-%m-%d %H:%M:%S", st_times)

        et_times = time.localtime(et_time)
        et_format_time = time.strftime("%Y-%m-%d %H:%M:%S", et_times)

        process_id = os.getpid()

        sql = "insert into api_result_log (media_name,interface_name,data_date,invoke_start_time," \
              "invoke_end_time,process_id,process_param,account_count,total) values('{0}','{1}','{2}'," \
              "'{3}','{4}','{5}','{6}','{7}','{8}')".format(
            media_name,
            url,
            data_date,
            st_format_time,
            et_format_time,
            process_id,
            process_param,
            account_total,
            data_total
        )
        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        try:
            # 执行sql语句
            cursor.execute(sql)
            cursor.close()
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()

    def put_api_error_info(self, db, date, account, url, media_name, error_msg, code):
        """将错误信息记录到mysql"""
        local_times = time.localtime(time.time())
        local_time_format = time.strftime("%Y-%m-%d %H:%M:%S", local_times)

        # data_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        data_date = date

        process_id = os.getpid()

        process_param = '--date={0} --account_key={1} --url={2}'.format(date, account, url)

        sql = "insert into api_error_log (media_name,interface_name,account_id,error_log_time," \
              "error_log_info,error_log_code,error_data_date,error_process_id,error_process_param) " \
              "values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')".format(
            media_name,
            url,
            account,
            local_time_format,
            error_msg,
            code,
            data_date,
            process_id,
            process_param
        )
        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        try:
            # 执行sql语句
            cursor.execute(sql)
            cursor.close()
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()

    @staticmethod
    def close_mysql_conn(db):
        logging.info('mysql connection closed')
        db.close()
