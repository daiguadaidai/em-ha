#!/usr/bin/env python
#-*- coding:utf-8 -*-

from emha import emha_agent
import unittest
import sys
import time


reload(sys)
sys.setdefaultencoding('utf8')

class TestEMHAAgent(unittest.TestCase):
    """EMHAManager类的单元测试"""

    def setUp(self):
        self.emha_agent = emha_agent.EMHAAgent()
        self.emha_agent.conn(hosts='127.0.0.1:2181')
        self.emha_agent.agent_identifier(
            host = '10.10.10.11',
            port = '3306',
            typ = '',
            project = 'Order_Item',
            room = 'xg',
        )

    def tearDown(self):
        self.emha_agent.stop()

    def test_register(self):
        """测试初始化节点"""

        project = self.emha_agent.project
        node = self.emha_agent.agent_name
        value = node
        self.emha_agent.register(project, node, value)

    def test_election(self):
        """测试选举"""

        self.emha_agent.election()

    def test_leader_watch_children(self):
        """测试监听Leader的变化"""

        self.emha_agent.leader_watch_children()

    def test_start(self):
        """测试start"""

        self.emha_agent.start()


if __name__ == '__main__':
    unittest.main()
