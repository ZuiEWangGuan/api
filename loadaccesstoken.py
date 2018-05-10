import pymysql
import json
from retrying import retry
from readconfig import Read
from handleconn import Conn
from sogouUtils import Utils
from loggers.logger import Logger

class UpdateToken:

    def __init__(self):
        """初始化mysql参数"""
        self.mysql_host = Read().mysql_host
        self.mysql_port = Read().mysql_port
        self.mysql_user = Read().mysql_user
        self.mysql_passwd = Read().mysql_passwd
        self.mysql_db = Read().mysql_db
        """调方法一定要加()"""
        self.db = self.getMysqlConn()
        self.redis_pool = Conn.getRedisConn()
        self.yeaterday = Utils().yeaterday

    #异常重试
    @retry(stop_max_attempt_number=10, wait_fixed=0.5)
    def getMysqlConn(self):
        """配置MySQL参数"""
        config = {
            'host': self.mysql_host,
            'port': self.mysql_port,
            'user': self.mysql_user,
            'passwd': self.mysql_passwd,
            'db': self.mysql_db,
            'charset': 'utf8'
        }
        # 打开数据库连接
        db = pymysql.connect(**config)
        return db

    def updateAgencys(self):
        # 使用cursor()方法获取操作游标
        cursors = self.db.cursor()
        try:
            #执行SQL语句
            cursors.execute("SELECT id,user,agency_ext from agencys WHERE media_id=9 AND is_import_agency=0 ")
            #获取全部数据
            results = cursors.fetchall()
            for row in results:
                """遍历将代理商对应的token写到redis里"""
                access_token = json.loads(row[2])['token']
                self.redis_pool.hset('sogou_access_token', row[0], access_token)
                Logger().inilg('update','{} token update success'.format(access_token))
                self.redis_pool.hset('sogou_agencysname', row[0], row[1] + '_sogou123')
                Logger().inilg('update','{} agencysname update success '.format(str(row[1])))
        except:
            Logger().inilg('update','{}  update failed '.format(str(row[0])))
            #关闭数据库连接
            self.db.close()

    def updateAccount(self):
        # 使用cursor()方法获取操作游标
        cursors = self.db.cursor()
        try:
            # 执行SQL语句
            # cursors.execute("SELECT account_fullname from account WHERE media_id=9 ")
            cursors.execute(
                "SELECT account_fullname from account WHERE media_id=9 AND  account_fullname regexp '^[1-9A-Za-z]'")
            # 获取全部数据
            results = cursors.fetchall()
            for row in results:
                """遍历将代理商对应的token写到redis里"""
                self.redis_pool.zadd('sogou_account_name_{}'.format(self.yeaterday), row[0], 14)
                Logger().inilg('update', '{} account update success'.format(row[0]))
            accountNumber=self.redis_pool.zcard('sogou_account_name_{}'.format(self.yeaterday))
            Logger().inilg('update','account number is {}'.format(str(accountNumber)))
        except:
            Logger().inilg('update','{} access update failed '.format(row[0]))
            # 关闭数据库连接
            self.db.close()

if __name__ == '__main__':
    """更新代理商id,userName,token信息"""
    UpdateToken().updateAgencys()
    Logger().inilg('update', '----------------------------agsName update complete')
    """更新广告主fullName"""
    UpdateToken().updateAccount()
    Logger().inilg('update', '----------------------------accountName update complete')
