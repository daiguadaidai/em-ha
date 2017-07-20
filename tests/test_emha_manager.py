#!/usr/bin/env python
#-*- coding:utf-8 -*-

from emha import emha_manager
import unittest
import sys
import time


reload(sys)
sys.setdefaultencoding('utf8')


class TestEMHAManager(unittest.TestCase):
    """EMHAManager类的单元测试"""

    def test_init_nodes(self):
        """测试初始化节点"""

        emha_mgr = emha_manager.EMHAManager()
        emha_mgr.conn_zk(hosts='127.0.0.1:2181')
        emha_mgr.init_nodes()

    def test_watch_mysql_clusters_children(self):
        """测试 MySQL 集群节点监听"""

        cluster_path = '/em-ha/mysql-clusters'

        emha_mgr = emha_manager.EMHAManager()
        emha_mgr.conn_zk(hosts='127.0.0.1:2181')
        emha_mgr.watch_mysql_clusters_children(path = cluster_path)

        time.sleep(100)

      


if __name__ == '__main__':
    unittest.main()
