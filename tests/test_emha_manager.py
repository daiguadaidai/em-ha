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

    def setUp(self):
        self.emha_mgr = emha_manager.EMHAManager()
        self.emha_mgr.conn(hosts='127.0.0.1:2181')

    def test_init_nodes(self):
        """测试初始化节点"""

        self.emha_mgr.init_nodes()

    def test_watch_mysql_clusters_children(self):
        """测试 MySQL 集群节点监听"""

        cluster_path = '/em-ha/mysql-clusters'

        self.emha_mgr.watch_mysql_clusters_children(path = cluster_path)

    def test_register(self):
        """测试选举注册"""
        self.emha_mgr.register()

    def test_election(self):
        self.emha_mgr.election()
      
    def test_election(self):
        self.emha_mgr.manager_watch_children()

    def test_mgr_queue(self):
        self.emha_mgr.init_mgr_queue()
        self.emha_mgr.do_queue_once()

    def test_start_manager(self):
        """测试完整的manager启动流程"""

        # 初始化节点
        self.emha_mgr.init_nodes()
        # 监听MySQL集群节点
        self.emha_mgr.watch_mysql_clusters_children()
        # 注册 Manager
        self.emha_mgr.register()
        # 选举 Leader
        self.emha_mgr.election()
        # 对leader节点进行监听
        self.emha_mgr.manager_watch_children()

        self.emha_mgr.init_mgr_queue()
        self.emha_mgr.do_queue()


if __name__ == '__main__':
    unittest.main()
