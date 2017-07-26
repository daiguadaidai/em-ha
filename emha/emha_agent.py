#!/usr/bin/env python
#-*- coding:utf-8 -*-

from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from emha_path import EMHAPath
import simplejson as json
import time
import os
import sys
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(module)s:%(lineno)s %(funcName)s - %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S'
)


class EMHAAgent(object):
    """EMHA Agent 用于监控MySQL是否存活。并且对MySQL进行切换"""

    zk = None
    is_leader = False

    def conn(self, hosts=""):
        """链接zk
        Args:
            hosts: 数据源
                example: 10.10.10.1:2181,10.10.10.1:2182,10.10.10.1:2183
        Return None
        Raise: None
        """

        if self.zk:
            self.zk.stop()

        self.zk = KazooClient(hosts=hosts)
        self.zk.start()

    def disconn(self):
        """断开zk的链接。这样所有的监听都会失效"""
        if self.zk:
            self.zk.stop()

    def start(self):
        """启动EMHA Agent"""
        pass

    def stop(self):
        """停止EMHA Agent"""

    def restart(self):
        """重启EMHA Agent"""
        self.stop()
        self.start()

    def create_node(self, path=None, value=None, ephemeral=False,
                          sequence=False, makepath=False):
        """创建EMHA zk节点
        Args:
            path: zk的节点路径
            value: 节点路径对于的值
        Return: True/False
        Raise: None
        """

        if self.zk.exists(path):
            print_str = '[{path}] exists!'.format(path = path)
            logging.warn(print_str)
            return False

        self.zk.create(path=path, value=value, ephemeral=ephemeral,
                       sequence=sequence, makepath=makepath)
        print_str = '[{path}] not exists, created it!'.format(path = path)
        logging.info(print_str)

        return True

    def notify_mgr(self, value=None):
        """通知Manager有东西需要处理了
        将相关信息put到 Manager Queue 中
        Args:
            value: put 的信息
        """
        mgr_queue = self.zk.Queue(EMHAPath.emha_nodes['mgr_queue']['path'])
        mgr_queue.put(value)

    def register(self, project, node, value):
        """为MySQL实例注册临时节点
        Args:
            project: 添加的是哪个集群
            node: 需要注册节点名称
            value: 节点存放的值
        Return: None
        Raise: None
        """

        # MySQL集群节点名称
        cluster_node = '{path}/{project}'.format(
                           path = EMHAPath.emha_nodes['mysql_clusters']['path'],
                           project = project)
        # MySQL实例节点名称
        mysql_node = '{cluster_node}/{node}'.format(
                           cluster_node = cluster_node,
                           node = node)

        logging.info('register MySQL instance node: {path}'.format(path=mysql_node))
        logging.info('MySQL instance node value: {value}'.format(value=value))

        # 配置是否是否已经有MySQL Cluster 节点
        if not self.zk.exists(path=cluster_node):
            data = {
                'action': 11,
                'node_name': project,
            }
            json_data = json.dumps(data)
            self.notify_mgr(value=json_data)

        # 等待 Manager 创建 MySQL Cluster 节点
        while not self.zk.exists(path=cluster_node):
            logging.warn('Cluster node dose not exits. waitting...')
            time.sleep(1)

        ok = self.create_node(path=path, value=value, ephemeral=True)

        if not ok:
            logging.err('register MySQL instance failure: {path}'.format(path=path))
            self.stop()

        logging.info('register MySQL instance Successful.')
