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

class EMHAManager(object):
    """EMHA 管理类
    action
        0: 没有标记
        1: 节点添加
        2: 节点删除
    """

    zk = None
    mysql_cluster_children_watchs = {}
    is_leader = False

    def __init__(self):
        pass

    def conn_zk(self, hosts=""):
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

    def create_node(self, path=None, value=None):
        """创建EMHA zk节点
        Args:
            path: zk的节点路径
            value: 节点路径对于的值
        Return: None
        Raise: None
        """

        if self.zk.exists(path):
            print_str = '[{path}] exists!'.format(path = path)
            logging.info(print_str)
        else:
            self.zk.create(path = path, value = value)
            print_str = '[{path}] not exists, created it!'.format(path = path)
            logging.warn(print_str)

    def put_queue(self, path=None, value=None):
        """将数据添加到队列中
        Args:
            path: 队列的路径
            value: 保存的数据一般来说一个json字符串
                example: {'nodes': [node1, node2, node3,]}
        return: None
        Raise: None
        """

        q = self.zk.Queue(path)
        q.put(value)
        logging.info('queue: {path}'.format(path=path))
        logging.info('value: {value}'.format(value=value))

    def mysql_cluster_children_watch_op(self, children, event):
        """对MySQL Cluster 节点的监听操作
        当MySQL cluster节点发生变化了。真正需要做什么
        """
        logging.info('running mysql_cluster_children_watch_op...')

        if not event: # 第一次注册监听，直接返回
            logging.warn('register watch node event')
            logging.warn('children: {children}'.format(children=children))
            return

        if not self.is_leader: # 如果是leader则添加队列
            logging.warn('current manager not leader. not put queue.')
            return
       

        ## 将事件发送到相关集群中的队列中
        # 1. 创建队列节点
        cluster_node = os.path.basename(event.path)
        cluster_queue = '/em-ha/agent-queue/{node}'.format(node=cluster_node)
        node_value = 'cluster: {node} queue'.format(node = cluster_node)
        self.create_node(path=cluster_queue, value=node_value)

        # 2. 添加队列
        value = {
            'nodes': children,
            'action': 0,
        }
        json_value = json.dumps(value)
        self.put_queue(cluster_queue, json_value)
        
        

    def mysql_cluster_children_watch(self, path=None, watch_name=None):
        """对MySQL cluster 节点进行监听
        Args:
            path: 需要监听的节点
            watch_name: 保存在 mysql_cluster_children_watchs 变量中的名称
        """

        logging.info('Path: {path}'.format(path=path))

        if not watch_name:
            watch_name = os.path.basename(path)
            logging.warn('not watch_name')

        logging.info('create watch_name: {watch_name}'.format(watch_name=watch_name))

        watch = ChildrenWatch(self.zk, path,
                              func = self.mysql_cluster_children_watch_op,
                              send_event = True)

        self.mysql_cluster_children_watchs[watch_name] = watch

        logging.info('watch {path} successsful'.format(path=path))

    def watch_mysql_clusters_children(self, path=None):
        """监听所有的 MySQL Cluster 子节点的增删变化
        
        Args:
            path: MySQL 集群节点. 没有特殊指定为则为 /em-ha/mysql-clusters
                  对该节点下的所有节点进行循环添加监听
        Return: None
        Raise: None
        """

        if not path:
            path = '/em-ha/mysql-clusters'

        mysql_clusters = self.zk.get_children(path) # 获取MySQL Cluster所有的节点

        for mysql_cluster in mysql_clusters: # 循环添加节点监听
            mysql_cluster_path = '{path}/{node}'.format(path = path,
                                                        node = mysql_cluster)
            self.mysql_cluster_children_watch(mysql_cluster_path, mysql_cluster)

    def init_nodes(self):
        """初始化需要使用zk节点"""

        if not self.zk:
            logging.error('zk not found, can not init emha path')
            return False

        for node_name, item in EMHAPath.emha_nodes.iteritems():
            self.create_node(**item)

    def register(self, node_name=None, path=None, value=None):
        """注册Manager到Zk中
        Arg:
            node_name: Manager节点名称,如果没有指定将为主机名 hostname-时间戳
            path: Manager 注册的节点路径
            value: 保存的数据
        Return: None
        Raise: None
        """
        pass

def main():
    pass


if __name__ == '__main__':
    main()
