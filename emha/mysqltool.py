#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(module)s:%(lineno)s %(funcName)s - %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S'
)

class MySQLTool(object):
    """对MySQL操作的一些命令"""

    def is_alive(self, host, port, username, password, tag=''):
        """检查MySQL是否连接的上"""
         
        is_alive = False

        logger.info('checking MySQL is alive [{host}, {port}]....'.format(
                     host = host, port = port))
        try:
            conn = MySQLdb.connect(**db_conf)
            logger.error('[{tag}] MySQL is Alive.'.format(tag=tag))
            is_alive = True
        except:
            error_msg = traceback.format_exc()
            logger.error('[{tag}] MySQL Connect Error.'.format(tag=tag))
            logger.error(error_msg)
        finally:
            conn.close()

        return is_alive

    def failover(self, new_master, slaves):
        """进行MySQL Failover"""
