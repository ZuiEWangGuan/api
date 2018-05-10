import configparser

class Read:

    cf = configparser.ConfigParser()

    cf.read('config.ini', encoding='utf-8')

    redis_host = cf.get('db', 'redis_host')
    redis_port = cf.get('db', 'redis_port')
    redis_passwd = cf.get('db', 'redis_passwd')

    hbase_host = cf.get('db', 'hbase_host')

    mysql_host = cf.get('db', 'mysql_host')
    mysql_port = int(cf.get('db', 'mysql_port'))
    mysql_user = cf.get('db', 'mysql_user')
    mysql_passwd = cf.get('db', 'mysql_passwd')
    mysql_db = cf.get('db', 'mysql_db')

    mysql_log_host = cf.get('db', 'mysql_log_host')
    mysql_log_user = cf.get('db', 'mysql_log_user')
    mysql_log_passwd = cf.get('db', 'mysql_log_passwd')
    mysql_log_db = cf.get('db', 'mysql_log_db')





