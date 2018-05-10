import time
import os
import pymysql
from retrying import retry
from datetime import datetime
from readconfig import Read
from handleconn import Conn
class Write:

    def __init__(self):
        """请求接口起始时间"""
        self.st_time = time.time()
        """媒体名称"""
        self.MEDIA_NAME = 'sogou'
        self.pool=Conn().redis_pool

    @retry(stop_max_attempt_number=10, wait_fixed=0.5)
    def getMysqlConn(self):
        # 打开数据库连接
        db = pymysql.connect(Read().mysql_log_host, Read().mysql_log_user
                             , Read().mysql_log_passwd, Read().mysql_log_db)
        return db

    def putData(self, db, sql):

            cursor = db.cursor()

            # 执行sql语句
            cursor.execute(sql)
            cursor.close()
            # 提交到数据库执行
            db.commit()

            # 如果发生错误则回滚
            db.rollback()

    def closeMysqlConn(self,db):
        db.close()


    def recordErrorLog(self, date, account, url, code, error_msg):
        """将错误信息记录到mysql"""
        local_times = time.localtime(time.time())
        local_time_format = time.strftime("%Y-%m-%d %H:%M:%S", local_times)

        data_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

        process_id = os.getpid()


        process_param = '--date={0} --account_key={1} --url={2}'.format(date, account, url)

        sql = "insert into api_error_log (media_name,interface_name,account_id,error_log_time,error_log_info,error_log_code,error_data_date,error_process_id,error_process_param) values('{0}','{1}','{2}','{3}',\"{4}\",'{5}','{6}','{7}','{8}')".format(
            self.MEDIA_NAME,
            url,
            account,
            local_time_format,
            error_msg,
            code,
            data_date,
            process_id,
            process_param
        )
        self.putData(self.getMysqlConn(), sql)

    def start_info(self, date, account_key, url):

        data_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

        st_times = time.localtime(self.st_time)
        st_format_time = time.strftime("%Y-%m-%d %H:%M:%S", st_times)

        process_id = os.getpid()

        process_param = '--date={0} --account_key={1} --url={2}'.format(date, account_key, url)

        sql = "insert into api_result_log (media_name,interface_name,data_date,invoke_start_time," \
              "process_id,process_param) values('{0}','{1}','{2}'," \
              "'{3}','{4}','{5}')".format(
            self.MEDIA_NAME,
            url,
            data_date,
            st_format_time,
            process_id,
            process_param
        )
        self.putData(self.getMysqlConn(), sql)

    def putApiStatisticsInfo(self,url, requests):

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
        self.putData(self.getMysqlConn(), sql)


    def end_info(self, account_total, data_total,date, account_key, url):
        et_time = time.time()

        data_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

        st_times = time.localtime(self.st_time)
        st_format_time = time.strftime("%Y-%m-%d %H:%M:%S", st_times)

        et_times = time.localtime(et_time)
        et_format_time = time.strftime("%Y-%m-%d %H:%M:%S", et_times)

        process_id = os.getpid()

        process_param = '--date={0} --account_key={1} --url={2}'.format(date, account_key, url)

        sql = "insert into api_result_log (media_name,interface_name,data_date,invoke_start_time," \
              "invoke_end_time,process_id,process_param,account_count,total) values('{0}','{1}','{2}'," \
              "'{3}','{4}','{5}','{6}','{7}','{8}')".format(
            self.MEDIA_NAME,
            url,
            data_date,
            st_format_time,
            et_format_time,
            process_id,
            process_param,
            account_total,
            data_total
        )
        self.putData(self.getMysqlConn(), sql)
        self.closeMysqlConn(self.getMysqlConn())
        self.st_time = time.time()


    def readMysqlData(self):

        local_times = time.localtime(time.time())
        local_date_format = time.strftime("%Y-%m-%d", local_times)
        print(local_date_format)
        """统计一下请求的总数"""
        db=self.getMysqlConn()
        cursor = db.cursor()
        #执行sql语句
        cursor.execute("select sum AS requestcount from api_statistics_log WHERE ymd={}".format(local_date_format))

        result = sum(int(x[0]) for x in cursor.fetchall())

        self.pool.set('gdt_requests_sum{}'.format(local_date_format),result)

if __name__ == '__main__':
    Write().readMysqlData()
