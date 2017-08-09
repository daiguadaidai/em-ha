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
import socket

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
        1: 有节点变动
        2: 有节点加入
        3: 有节点删除
    """

    zk = None
    mysql_cluster_children_watchs = {}
    leader_watch = None
    is_leader = False
    mgr_name = '{name}-{pid}-{ts}'.format(name = socket.gethostname(),
                                          pid = os.getpid(),
                                          ts = int(time.time() * 1000))

    def __init__(self):
        pass

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
            logging.info(print_str)
            return False

        self.zk.create(path=path, value=value, ephemeral=ephemeral,
                       sequence=sequence, makepath=makepath)
        print_str = '[{path}] not exists, created it!'.format(path = path)
        logging.warn(print_str)

        return True

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
            logging.warn('register watch MySQL Cluster node event')
            logging.warn('MySQL Cluster nodes: {children}'.format(children=children))
            return

        if not self.is_leader: # 如果是leader则添加队列
            logging.warn('current manager not leader. not put queue.')
            return
       

        ## 将事件发送到相关集群中的队列中
        # 1. 创建队列节点
        cluster_node = os.path.basename(event.path)
        cluster_queue = '{queue_path}/{node}'.format(queue_path = EMHAPath.emha_nodes['agent_queue']['path'],
                                                     node = cluster_node)
        node_value = 'cluster: {node} queue'.format(node = cluster_node)
        self.create_node(path=cluster_queue, value=node_value)

        # 2. 添加队列
        value = {
            'nodes': children,
            'action': 1,
        }
        json_value = json.dumps(value)
        self.put_queue(cluster_queue, json_value)
        
    def mysql_cluster_children_watch(self, path=None, watch_name=None):
        """对MySQL cluster 节点进行监听
        Args:
            path: 需要监听的节点
            watch_name: 保存在 mysql_cluster_children_watchs 变量中的名称
        """

        logging.info('watch MySQL Cluster Path: {path}'.format(path=path))

        if not watch_name:
            watch_name = os.path.basename(path)
            logging.warn('not watch_name')

        logging.info('create watch_name: {watch_name}'.format(watch_name=watch_name))

        watch = ChildrenWatch(self.zk, path,
                              func = self.mysql_cluster_children_watch_op,
                              send_event = True)

        self.mysql_cluster_children_watchs[watch_name] = watch

        logging.info('watch MySQL Cluster successsful: {path}'.format(path=path))

    def watch_mysql_clusters_children(self, path=None):
        """监听所有的 MySQL Cluster 子节点的增删变化
        
        Args:
            path: MySQL 集群节点. 没有特殊指定为则为 /em-ha/mysql-clusters
                  对该节点下的所有节点进行循环添加监听
        Return: None
        Raise: None
        """

        if not path:
            path = EMHAPath.emha_nodes['mysql_clusters']['path']

        mysql_clusters = self.zk.get_children(path) # 获取MySQL Cluster所有的节点

        logging.info('init mysql clusters watcher: {path}'.format(path=path))
        logging.info('MySQL Clusters: {nodes}'.format(nodes=str(mysql_clusters)))
        for mysql_cluster in mysql_clusters: # 循环添加节点监听
            mysql_cluster_path = '{path}/{node}'.format(path = path,
                                                        node = mysql_cluster)
            self.mysql_cluster_children_watch(mysql_cluster_path, mysql_cluster)

    def leader_watch_children_op(self, children, event):
        """监听 Manager 节点的增删变化
        如果有发生变化就开始进行选举 Leader 操作
        """
        if not event: # 第一次注册监听，直接返回
            logging.warn('register watch Manager node event')
            logging.warn('Managers: {children}'.format(children=children))
            return

        logging.warn("watched Manager Leader node change.")

        # 参与 Leader 选举
        ok = self.election()

        if not ok:
            return

        # 监听mysqlcluster
        self.watch_mysql_clusters_children()
        
    def leader_watch_children(self, path=None):
        """监听 Manager 节点的子节点增删"""

        if not path:
            path = EMHAPath.emha_nodes['mgr_leader']['path']

        logging.info('Manager Leader watch path: {path}'.format(path=path))

        watch = ChildrenWatch(self.zk, path,
                              func = self.leader_watch_children_op,
                              send_event = True)

        self.leader_watch = watch

    def election(self, path=None, identifier=None):
        """Manager Leader 的选举
        Args
            path: Manager 选举 Leader 使用的节点
            identifier: 该参与选举的 Manager 名称
        Return: None
        Raise: None
        """

        # 如果已经是Leader则直接退出不参加选举
        if self.is_leader:
            logging.warn("current Manager is Leader. do not attend election.")
            return False
         

        # 设置 Manager 选举路径
        if not path:
            path = EMHAPath.emha_nodes['mgr_leader_election']['path']
            logging.warn('not found Manager election leader path, use default.')
        logging.info('Manager election path: {path}'.format(path=path))

        # 设置 Manager 选举标识
        if not identifier:
            identifier = self.mgr_name
        
        semaphore = self.zk.Semaphore(path, identifier=identifier)

        semaphore.acquire(blocking=True, timeout=5)
        # 如果日志只有 start election 而没有 end election 代表有hang现象需要找出锁住的进程从而kill进程
        logging.info('--------------------------start election -----------------------------')

        leader_name = self.mgr_name
        logging.info('leader name: {leader_name}'.format(leader_name=leader_name))
        
        leader_path = EMHAPath.emha_nodes['mgr_leader']['path']
        logging.info('leader path: {leader_path}'.format(leader_path=leader_path))

        leaders = self.zk.get_children(leader_path)
        logging.info('current leaders: {leaders}'.format(leaders=str(leaders)))

        # Leader 已经存在则返回
        if len(leaders) > 0:
            logging.warn('leader exists.')
            semaphore.release()
            logging.info('--------------------------end election -------------------------------')
            return False

        # 创建 Leader 节点
        leader_node = '{leader_path}/{leader_name}'.format(leader_path = leader_path,
                                                           leader_name = leader_name)

        if self.create_node(path=leader_node, value=leader_name, ephemeral=True):
            logging.info('leader node created.')
        else:
            logging.info('leader node create failure.')
            semaphore.release()
            logging.info('--------------------------end election -------------------------------')
            return False

        # 设置当前 Manager 为 Leader
        self.is_leader = True

        semaphore.release()

        logging.info('--------------------------end election -------------------------------')
        return True

    def init_nodes(self):
        """初始化需要使用zk节点"""

        if not self.zk:
            logging.error('zk not found, can not init emha path')
            return False

        # 初始化根节点
        self.create_node(**EMHAPath.root_node['root_path'])

        # 初始化二级节点
        for node_name, item in EMHAPath.emha_nodes.iteritems():
            self.create_node(**item)

    def register(self, node_name=None, path=None, value=None):
        """注册Manager到Zk中
        1. 创建临时节点以hostname-pid-时间戳为节点名称
        2. 选举leader
        3. 监听管理节点的节点增加删除变化
        Arg:
            node_name: Manager节点名称,如果没有指定将为主机名 hostname-时间戳
            path: Manager 注册的节点路径
            value: 保存的数据
        Return: None
        Raise: None
        """

        # 1. 创建临时节点以hostname-时间戳为节点名称
        if not node_name: # 生成 Mananger节点名称
            node_name = self.mgr_name
            logging.warn('Manager node name not find, create it.')
        logging.info('Manager node name: {name}'.format(name=node_name))

        # 生成 Manager 路径
        if not path:
            path = EMHAPath.emha_nodes['manager']['path']
            logging.warn('no found manager path, use default.')
        logging.info('Manager path: {path}'.format(path=path))

        # 生成Manager 值
        if not value:
            value = node_name

        manager_node = '{path}/{name}'.format(path=path, name=node_name)
        logging.info('Manager node: {node}'.format(node=manager_node))

        # Manager注册临时节点
        if not self.create_node(path=manager_node, value=value, ephemeral=True):
            logging.error('Manager register failure.')
            return False
            
        logging.info('Manager register Successful.')

    def init_mgr_queue(self, path=None):
        """初始化管理节点(Manager)队列变量
        Args:
           mgr_queue: Manager队列路径
        """
        if not path:
            path = EMHAPath.emha_nodes['mgr_queue']['path']
            logging.warn('no found manager queue path, use default.')
        logging.info('Manager queue path: {path}'.format(path=path))

        self.mgr_queue = self.zk.Queue(path)
        logging.info('init Manager queue.')

    def init_cluster_nodes(self, name):
        """初始化每个集群节点需要的节点
        Args:
            name: 集群节点的名称
        Return: None
        Raise: None
        """
        logging.info('1. craete MySQL Cluster node')
        logging.info('2. craete agent queue node')
        logging.info('3. craete cluster master node')
        logging.info('4. create agent_leader node')
        logging.info('5. craete cluster update_cluster_data_lock node')
        logging.info('6. craete agent_working node')
        logging.info('7. craete cluster priority_machine_room node')
        logging.info('8. craete cluster nultiple live type node')
        logging.info('9. add watcher to new MySQL Cluster node')
        
        mysql_cluster_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['mysql_clusters']['path'],
                                   node = name))
        agent_queue_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['agent_queue']['path'],
                                   node = name))
        cluster_master_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['cluster_master']['path'],
                                   node = name))
        agent_leader_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['agent_leader']['path'],
                                   node = name))
        update_cluster_data_lock_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['update_cluster_data_lock']['path'],
                                   node = name))
        agent_warking_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['agent_working']['path'],
                                   node = name))
        priority_machine_room_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['priority_machine_room']['path'],
                                   node = name))
        agent_leader_election_node = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['agent_leader_election']['path'],
                                   node = name))
        agent_mul_live_type = (
            '{path}/{node}'.format(path = EMHAPath.emha_nodes['mul_live_type']['path'],
                                   node = name))

        # 创建 MySQL Cluster Master 节点
        self.create_node(path=agent_queue_node, value=name)
        # 创建 MySQL Agent Queue 节点
        self.create_node(path=cluster_master_node, value=name)
        # 创建 Agent Leader Node 节点
        self.create_node(path=agent_leader_node, value=name)
        # 创建 更新MySQL集群数据锁节点
        self.create_node(path=update_cluster_data_lock_node, value=name)
        # 创建 agent 正在工作的节点
        self.create_node(path=agent_warking_node, value=name)
        # 创建 优先使用机房节点
        self.create_node(path=priority_machine_room_node, value=name)
        # 创建 优先使用机房节点
        self.create_node(path=agent_leader_election_node, value=name)
        # 创建 集群多活类型
        self.create_node(path=agent_mul_live_type, value=name)

        # 初始化集群节点数据
        cluster_data = {
            'machine_rooms': {},
        }
        cluster_json_data = json.dumps(cluster_data)
        # 创建 MySQL 集群节点
        self.create_node(path=mysql_cluster_node, value=cluster_json_data)
   
    def do_queue_once(self):
        """Manager处理一次队列
        Other:
        queue data foramt: "{'action': 11, 'node_name': 'cluster03'}"
        """
        # data is json format
        data = self.mgr_queue.get()

        if not data:
            return None

        info = json.loads(data)

        logging.info('mgr queue action is: {action}'.format(action=info['action']))

        if int(info['action']) == 11:
            # 初始化需要的集群节点
            self.init_cluster_nodes(name = info['node_name'])

            mysql_cluster_node = (
                '{path}/{node}'.format(path = EMHAPath.emha_nodes['mysql_clusters']['path'],
                                       node = info['node_name']))
            # 监听新节点
            self.mysql_cluster_children_watch(path=mysql_cluster_node)
        else:
            logging.warn('Manager no do queue: {data}'.format(data=data))

        return True

    def do_queue(self):
        """不停的获取queue并且进行操作"""

        ok = False
        none_op_cnt = 0
        while True:

            # 当前不是leader则不执行
            if not self.is_leader:
                none_op_cnt = 0
                logging.warn('Current Manager is not Leader')
                time.sleep(1)
                continue
            

            # 当空操作为操过 100 次就每操作一次睡眠一次
            if none_op_cnt >= 100:
                logging.warn('Manager deal queue None operation, operation count: {cnt}'.format(cnt=none_op_cnt))
                time.sleep(1)
            
            ok = self.do_queue_once()

            if ok:
                logging.info('Manager deal queue Successful!')
                none_op_cnt = 0
            else:
                none_op_cnt += 1


def main():
    pass


if __name__ == '__main__':
    main()
