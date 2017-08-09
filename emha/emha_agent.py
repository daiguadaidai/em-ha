#!/usr/bin/env python
#-*- coding:utf-8 -*-

from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch
from emha_path import EMHAPath
from mysqltool import MySQLTool
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
        self.recovery()
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

    def remove_node(self, path=None):
        """删除节点"""

        if not self.zk.exists(path):
            print_str = '[{path}] dose not exists!'.format(path = path)
            logging.warn(print_str)
            return False

        self.zk.delete(path)
        logging.info('{node} is removed.'.format(node=path))
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
        action_type = None

        data, znodestat = self.zk.get(path)

        cluster_info = json.loads(data)
        instances = self.get_cluster_nodes(cluster_info)

        logging.info('cluster info: {info}'.format(info=cluster_info))
        logging.info('old instances: {instances}'.format(instances=instances))
        logging.info('current instances {instances}'.format(instances=nodes))

        if len(instances) > len(nodes):
            action_type = 'remove'
        elif len(instances) < len(nodes):
            action_type = 'add'

        logging.info('The Agent Action is: {tag}'.format(tag = action_type))

        node = list(set(instances) ^ set(nodes)) # 获取两个集合不同的节点

        # bug
        if node: # 暂时只能有一个Node的差别,
            node = node.pop()
        else:
            logging.warn('Diff Node Result: None. Cluster data already have this None')

        logging.info('{tag} Node is: {node}'.format(tag = action_type,
                                                    node = node))

        return action_type, node, cluster_info

    def get_cluster_nodes(self, cluster_data):
        """从集群数据中获得所有集群节点
        Args
            cluster_data: 集群信息
        """
        instances = []

        for room, typs in cluster_data['machine_rooms'].iteritems():
            for typ in typs:
               # 添加元素到一个list中
               if type(cluster_data['machine_rooms'][room][typ]) == str:
                   instances.append(cluster_data['machine_rooms'][room][typ])
               elif type(cluster_data['machine_rooms'][room][typ]) == list:
                   for instance in cluster_data['machine_rooms'][room][typ]:
                       instances.append(instance)

        return instances

    def instances2topo(self, nodes):
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

            # master只有一个节点
            if typ.lower() == 'master':
                new_cluster_info['machine_rooms'][room][typ] = node
                continue

            # 添加新实例类型
            if not new_cluster_info['machine_rooms'][room].has_key(typ):
                new_cluster_info['machine_rooms'][room][typ] = []

            # dt/slaves/delay 有多个节点
            new_cluster_info['machine_rooms'][room][typ].append(node)

        return new_cluster_info

    def save_data(self, path, data):
        """修改集群数据
        Args
            path: MySQL集群节点
            new_nodes: MySQL节点变化后的节点
        """
        data = json.dumps(data)
        self.zk.set(path, data)
        logging.info('save data to {path}: {data}'.format(path=path, data=data))

    def get_node_info(self, node, sep=':'):
        """分解节点获得相关信息"""
        return node.split(':')

    def node_is_master(self, node):
        """判断节点是否是Master"""
        if not node:
            return False

        project, room, typ, host, port = self.get_node_info(node)
        if typ.lower() == 'master':
            return True

        return False

    def exists_master(self, mul_live_type, node):
        """该集群是否有Master
        Arg
            mul_live_type: 集群类型
            node: 实例名称
        """
        # 获得Master节点
        path = '{path}/{project}'.format(
                path = EMHAPath.emha_nodes['cluster_master']['path'],
                project = self.project)

        masters = self.zk.get_children(path)

        logging.info('Current Master iS: {master}'.format(master = masters))

        if not masters: # 没有Master
            return False

        project, room, typ, host, port = self.get_node_info(node)

        if mul_live_type == 'drc': # DRC 模式
            for master in masters:
                master_project, master_room, master_typ, master_host, master_port = self.get_node_info(master)
                if master_room == room:
                    return True

            return False
             
        elif mul_live_type in ['gz', 'normal']: # Global Zone 模式
            return True
        else: # 普通集群模式
            return True

    def get_mul_live_type(self, mul_live_type_path=None):
        """获得多活类型"""
        # 获得集群类型
        if not mul_live_type_path:
            mul_live_type_path = '{path}/{project}'.format(
                                  path = EMHAPath.emha_nodes['mul_live_type']['path'],
                                  project = self.project)
        mul_live_type, znodestat = self.zk.get(path=mul_live_type_path)

        if mul_live_type.lower() not in ['drc', 'gz', 'normal']:
            logging.warn('Multiple Live Type is: {tpy}. Error Type'.format(tpy=mul_live_type))
            logging.warn('Multiple Live Type Redirector.')
            mul_live_type = 'normal'

        logging.info('Multiple Live Type is: {mul_live_type}'.format(mul_live_type=mul_live_type))       

        return mul_live_type

    def get_priority_room(self):
        """切换时优先选择的机房, 一般是针对 Global Zone 需要的.
        主要是为了判断第一个主的Master是哪个
        """
        path = '{path}/{project}'.format(
                                path = EMHAPath.emha_nodes['priority_machine_room']['path'],
                                project = self.project)
        room, znodestat = self.zk.get(path)
        return room

    def get_slaves(self, master, nodes):
        """通过Master和集群拓扑获得相关slave"""
        project, room, typ, host, port = self.get_node_info(master)
        slaves = []
        for instance in nodes:
            i_project, i_room, i_typ, i_host, i_port = self.get_node_info(instance)
            # 不在同一机房并不属于同一类型就是slave
            if room == i_room and typ != i_typ:
                slaves.append(instance)

        return slaves

    def add_master(self, node):
        """判断并且设置集群Master"""
        
        master_node = '{path}/{project}/{node}'.format(
                         path = EMHAPath.emha_nodes['cluster_master']['path'],
                         project = self.project,
                         node = node)
        self.create_node(path=master_node, value=node)
        logging.info('Create Master Node: {path} Successful...'.format(path = master_node))

    def remove_master(self, node):
        """移除Master"""
        master_node = '{path}/{project}/{node}'.format(
                         path = EMHAPath.emha_nodes['cluster_master']['path'],
                         project = self.project,
                         node = node)
        
        if not self.remove_node(master_node):
            logging.warn('Master dose not exists. remove fail')
            return False

        logging.warn('Master Remove Successful...')
        return True 

    def election_master(self, cluster_info, dead_node, mul_live_type):
        """从新的集群中选择出新的Master
        Args
            cluster_info: 现在所剩的所有实例
            dead_node: 宕机的节点
            mul_live_type: 多活类型
        """
        new_master_node = None

        project, room, typ, host, port = self.get_node_info(dead_node)

        # 如果没有 机房的信息直, 代表没有找到Master
        if not cluster_info.has_key('machine_rooms'):
            logging.error('cluster info Error, can not found key [machine_rooms]')
            return new_master_node

        # 是否存在slave, 从list中选举出最后一个Slave为新的Master
        if cluster_info['machine_rooms'][room].has_key('slave'):

            slave_count = len(cluster_info['machine_rooms'][room]['slave'])
            if slave_count > 0: # 有master可以选择
                new_master_node = cluster_info['machine_rooms'][room]['slave'][slave_count-1]
                logging.info('room [{room}] slave found: {master}'.format(
                                                       room = room,
                                                       master = new_master_node))
                return new_master_node
            else:
                logging.error('room [{room}] no slave found'.format(room=room))

        # 在上面Slave中没有选举到Master,需要判断是否存在 DT
        if cluster_info['machine_rooms'][room].has_key('dt'):
            dt_count = len(cluster_info['machine_rooms'][room]['dt'])
            if dt_count > 0: # 有master可以选择
                new_master_node = cluster_info['machine_rooms'][room]['dt'][dt_count-1]
                logging.info('room [{room}] dt slave found: {master}'.format(
                                                       room = room,
                                                       master = new_master_node))
                return new_master_node
            else:
                logging.error('room [{room}] no dt slave found'.format(room=room))

        logging.error('room [{room}] can not found new master.'.format(room=room))
        return new_master_node

    def slave2master(self, slave, nodes):
        """将slave转化为master"""
        project, room, typ, host, port = self.get_node_info(slave)
        master = '{project}:{room}:{typ}:{host}:{port}'.format(
                                           project = project,
                                           room = room,
                                           typ = 'master',
                                           host = host,
                                           port = port)
        logging.info('convert S[{slave}] to M[{master}]'.format(
                                                          slave = slave,
                                                          master = master))

        # 获得所有实例节点
        instances = list(set(nodes) - set([master]))
        # 添加新Master
        instances.append(master)
        # 获得新的拓扑信息
        cluster_info = self.instances2topo(instances)   

        return master, cluster_info

    def deal_action(self, action=0, nodes=[]):
        """通过给的数据处理相关信息
        Args
            action: 需要做什么操作, README中有接收各种标号代表什么
        """
        action = int(action)

        if action == 0:
            return

        if action == 1: # 有增删节点操作
            # 获取最新的集群信息
            cluster_info = self.instances2topo(nodes)   

            # 判断是增加还是删除
            action_type, node, cluster_info = self.diff_node(
                                  path = self.cluster_node_path,
                                  nodes = nodes)

            # 判断该节点是否是Master
            if not self.node_is_master(node):
                logging.info('The change Node is not Master. No Opration.')
                return

            logging.warn('The change Node is Master.......')

            # 获得多活类型
            mul_live_type = self.get_mul_live_type()

            if action_type == 'add': # 如果是新加节点需要设置master
                # 判断master是否存在 TODO
                if self.exists_master(mul_live_type, node):
                    logging.info('Master exists. Needn\'t Add.')
                    # 设置罪行集群信息
                    self.save_data(self.cluster_node_path, cluster_info)
                    return

                # 不存在Master则创建Master
                logging.info('Master dose not exits. create it')       
                self.add_master(node) # 设置Master

            elif action_type == 'remove': # 集群切换
                # 移除该Master
                self.remove_master(node)

                # 选出候选Master,此时该节点标识还不是master: project:room:slave/dt:host:port
                backend_master = self.election_master(cluster_info, node, mul_live_type)

                if not backend_master:
                    logging.error("Election New Master Fail. Please Deal With it")
                    # 需要进行告警
                else: 
                    # 获得新的MySQL拓扑
                    master, cluster_info = self.slave2master(backend_master, nodes)
                    slaves = self.get_slaves(master, nodes)
                    # 进行MySQL切换
                    mysql_tool = MySQLTool()
                    mysql_tool.failover(master, slaves)
                    self.add_master(master)
                

            # 设置罪行集群信息
            self.save_data(self.cluster_node_path, cluster_info)

    def recovery(self):
        """从工作区中恢复未完成的工作"""
        if not self.is_leader:
            return False

        logging.info('------------------ start recovery ------------------------')

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
            data, znodestat = self.zk.get(recovery_node)
            logging.info('recovery data: {data}'.format(data = data))
            json_data = json.loads(data)

            # 处理相关未完成工作
            self.deal_action(**json_data)
            # 处理完之后就可以删除相关的节点
            self.remove_node(recovery_node)
            logging.info('recovery node: {recovery_node} Successful'.format(recovery_node = recovery_node))

        logging.info('------------------ recovery end ------------------------')

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
