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
    agent_name = None
    leader_watch = None

    def agent_identifier(self, host='127.0.0.1', port=3306,
                               typ='', project='', room=''):
        """为该Agent添加标识
        Args
            host: 实例IP
            port: 端口
            typ: 实例类型
            project: 项目名
            room: 机房
        """

        self.host = host
        self.port = port
        self.typ = typ
        self.project = project
        self.room = room

        self.agent_name = '{project}:{room}:{typ}:{host}:{port}'.format(
            project = self.project,
            room = self.room,
            typ = self.typ,
            host = self.host,
            port = self.port,
        )

        self.cluster_node_path = '{path}/{project}'.format(
                           path = EMHAPath.emha_nodes['mysql_clusters']['path'],
                           project = project)

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
        self.zk.start()
        self.register(self.project, self.agent_name, self.agent_name)
        # 选举leader
        self.election()
        # 监听leader
        self.leader_watch_children()
        # 恢复工作
        # self.recovery()
        # 处理队列
        # self.do_queue()

    def stop(self):
        """停止EMHA Agent"""
        self.disconn()

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
        logging.info("notify Manager Successful. data: {data}".format(data=value))
        
    def leader_watch_children_op(self, children, event):
        """监听 Agent 节点的增删变化
        如果有发生变化就开始进行选举 Leader 操作
        """
        if not event: # 第一次注册监听，直接返回
            logging.warn('register watch Agent Leader node event')
            logging.warn('Leaders: {children}'.format(children=children))
            return

        logging.warn("watched Agent Leader node change.")

        # 参与 Leader 选举 如果选举成功将不设置本Agent为leader。
        # 主要是为了避免leader还没有进行recovery就先处理队列
        ok = self.election(is_change_leader=False)

        if not ok:
            return

        # 恢复Agent工作区
        # self.recovery()
        # 设置本Agent为Leader
        self.is_leader = True

    def leader_watch_children(self, path=None):
        """监听 Agent 节点的子节点增删"""

        if not path:
            path = '{path}/{project}'.format(
                    path = EMHAPath.emha_nodes['agent_leader']['path'],
                    project = self.project)

        logging.info('Agent Leader watch path: {path}'.format(path=path))

        watch = ChildrenWatch(self.zk, path,
                              func = self.leader_watch_children_op,
                              send_event = True)

        self.leader_watch = watch

    def diff_node(self, path, nodes=[]):
        """比较节点是增加还是删除
        Args
            path: MySQL集群节点
            nodes: MySQL节点变化后的节点
        Return
            action_type: 返回的节点变更类型
                         None: 无法判断变更类型。不进行操作
                         remove: 集群删除节点
                         add: 集群增加节点
            node: 增/删 的是哪个节点
            cluster_info: 当前的集群节点数据
        """
        # 获取集群节点的数据
        actioin_type = None

        data = self.zk.get(path)

        cluster_info = json.loads(data)
        instances = self.get_cluster_nodes(cluster_info)

        logging.info('cluster info: {info}'.format(info=cluster_info))
        logging.info('old nodes: {instances}'.format(instances=instances))
        logging.info('new current {nodes}: {nodes}'.format(nodes=nodes))

        if len(instances) > len(nodes):
            actioin_type = 'remove'
        elif len(instances) < len(nodes):
            actioin_type = 'add'

        logging.info('The Agent Action is: {tag}'.format(tag = action_type))

        node = set(instances) ^ len(nodes) # 获取两个集合不同的节点
        logging.info('{tag} Node is: '.format(tag = action_type,
                                              node = node))

        return actioin_type, node, cluster_info

    def get_cluster_nodes(self, cluster_data):
        """从集群数据中获得所有集群节点
        Args
            cluster_data: 集群信息
        """
        instances = []

        for room, typs in cluster_data['machine_rooms'].iteritems():
            for typ in typs:
               for instance in cluster_data['machine_rooms'][room][typ]:
                   instances.append(instance)

        return instances

    def get_new_cluster_data(self, nodes):
        """获取最新的集群节点数据
        Args
            nodes: 集群变更后的节点
        """

        new_cluster_info = {'machine_rooms':{}}

        # 重新生成新的进群信息数据
        for node in nodes:
            project, room, typ, host, port = node.split(':')

            # 添加新 机房
            if not new_cluster_info['machine_rooms'].has_key(room):
                new_cluster_info['machine_rooms'][room] = {}

            # 添加新实例类型
            if not new_cluster_info['machine_rooms'][room].has_key(typ):
                new_cluster_info['machine_rooms'][room][typ] = []

            new_cluster_info['machine_rooms'][room][typ].append(node)

        return new_cluster_info

    def save_data(self, path, data):
        """修改集群数据
        Args
            path: MySQL集群节点
            new_nodes: MySQL节点变化后的节点
        """
        self.zk.set(path, data)

    def has_master(self, mul_live_type, node):
        """该集群是否有Master
        Arg
            mul_live_type: 集群类型
            node: 新添加实例名
        """
        # 获得Master节点
        path = '{path}/{project}'.format(
                path = path = EMHAPath.emha_nodes['cluster_master']['path'],
                project = self.project)

        masters = self.zk.et_children(path)

        if not masters: # 没有Master
            return False

        project, room, typ, host, port = node.split(':')

        if mul_live_type == 'drc': # DRC 模式
            has = False
            for master in masters:
                master_project, master_room, master_typ, master_host, master_port = master.split(':')
                if master_room == room:
                    return True

            return False
             
        elif mul_live_type == 'gz': # Global Zone 模式
            return True
        else: # 普通集群模式
            return True

    def set_master(self, node):
        """判断并且设置集群Master"""

        # 获得集群类型
        mul_live_type_path = '{path}/{project}'.format(
                              path = EMHAPath.emha_nodes['mul_live_type']['path'],
                              project = self.project)
        mul_live_type = self.zk.get(path=mul_live_type_path)
        logging.info('Multiple Live Type is: {mul_live_type}'.format(mul_live_type=mul_live_type))       

        if not self.has_master(mul_live_type, node):
            logging.info('Master not exits. create it.')       
            self.create_node(path=node, value=node)
            return

        logging.info('Master already exits.')       

    def deal_action(self, action=0, nodes=[]):
        """通过给的数据处理相关信息
        Args
            action: 需要做什么操作, README中有接收各种标号代表什么
        """

        action = int(action)

        if action == 0:
            return

        if action == 1: # 有增删节点操作
            # 判断是增加还是删除
            action_type, node, cluster_info = self.diff_node(
                                  path = self.cluster_node_path,
                                  nodes = nodes)

            if action_type == 'add': # 如果是新加节点需要设置master
                # 判断master是否存在 TODO
                self.set_master(node)
            elif action_type == 'remove': # 集群切换
                self.remove_instance(node)

            # 获取最新的集群信息
            new_cluster_info = self.get_new_cluster_data(nodes)   
            # 设置罪行集群信息
            self.save_data(self.cluster_node_path, new_cluster_info)
            # 判断是否需要切换
            # TODO


    def recovery(self):
        """从工作区中恢复未完成的工作"""
        if not self.is_leader:
            return False

        logging.info('starting recovery cluster working...')

        # 获取所有需要恢复的节点
        working_path = '{path}/{project}'.format(
                                 path = EMHAPath.emha_nodes['agent_working']['path'],
                                 project = self.project)
        recovery_nodes = self.zk.get_children(working_path)
        recovery_nodes.sort()

        logging.info('Need Recovery Node Name: {nodes}'.format(nodes = recovery_nodes))

        for node in recovery_nodes: # 对工作区进行循环恢复
            recovery_node = '{working_path}/{node}'.format(working_path = working_path,
                                                           node = node)
            logging.info('recovery node: {recovery_node}'.format(recovery_node = recovery_node))
            data = self.zk.get(recovery_node)
            logging.info('recovery data: {data}'.format(data = data))
            json_data = json.loads(data)

            # 处理相关未完成工作
            self.deal_action(**json_data)
            # 处理完之后就可以删除相关的节点
            self.zk.delete(recovery_node)
            logging.info('recovery node: {recovery_node} Successful'.format(recovery_node = recovery_node))

    def do_queue(self):
        """获取队列信息。并且进行操作"""
        pass

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
        cluster_node = self.cluster_node_path

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

        ok = self.create_node(path=mysql_node, value=value, ephemeral=True)

        if not ok:
            logging.err('register MySQL instance failure: {path}'.format(path=path))
            self.stop()

        logging.info('register MySQL instance Successful.')

    def election(self, path=None, identifier=None, is_change_leader=True):
        """Agent Leader 的选举
        Args
            path: Agent 选举 Leader 使用的节点
            identifier: 该参与选举的 Agent 名称
        Return: None
        Raise: None
        """

        # 如果已经是Leader则直接退出不参加选举
        if self.is_leader:
            logging.warn("current Agent is Leader. do not attend election.")
            return False
         

        # 设置 Agent 选举路径
        if not path:
            path = '{election_path}/{project}'.format(
                election_path = EMHAPath.emha_nodes['agent_leader_election']['path'],
                project = self.project,
            )
            logging.warn('not found Agent election leader path, use default.')
        logging.info('Agent election path: {path}'.format(path=path))

        # 设置 Manager 选举标识
        if not identifier:
            identifier = self.agent_name
        
        semaphore = self.zk.Semaphore(path, identifier=identifier)

        semaphore.acquire(blocking=True)
        # 如果日志只有 start election 而没有 end election 代表有hang现象需要找出锁住的进程从而kill进程
        logging.info('-------------------------- start election -----------------------------')

        leader_name = self.agent_name
        logging.info('leader name: {leader_name}'.format(leader_name=leader_name))

        leader_path = '{path}/{project}'.format(
                       path = EMHAPath.emha_nodes['agent_leader']['path'],
                       project = self.project)
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
        leader_node = '{leader_path}/{leader_name}'.format(
                       leader_path = leader_path,
                       leader_name = leader_name)

        if self.create_node(path=leader_node, value=leader_name, ephemeral=True):
            logging.info('leader node created.')
        else:
            logging.info('leader node create failure.')
            semaphore.release()
            logging.info('--------------------------end election -------------------------------')
            return False

        # 设置当前 Manager 为 Leader
        if is_change_leader:
            self.is_leader = True

        semaphore.release()

        logging.info('--------------------------end election -------------------------------')
        return True
