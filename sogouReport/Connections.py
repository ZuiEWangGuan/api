from configparser import ConfigParser

import redis


class RedisConn:
    cfp = ConfigParser()
    cfp.read('SoGou.ini', encoding='utf-8')

    redis_host = cfp.get('redis', 'redis_host')
    redis_port = cfp.getint('redis', 'redis_port')
    redis_password = cfp.get('redis', 'redis_password')

    pool = redis.ConnectionPool(db=0, password=redis_password, host=redis_host, port=redis_port, max_connections=10)

    @staticmethod
    def get_redis_conn():
        return redis.Redis(connection_pool=RedisConn.pool)
