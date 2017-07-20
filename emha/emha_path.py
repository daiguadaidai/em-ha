#!/usr/bin/env python
#-*- coding:utf-8 -*-

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

class EMHAPath(object):
    """EMHA 需要路径信息"""

    #### 定义zk需要的节点 ####
    emha_nodes = {
        # EMHA root 节点
        'root_path': {
            'path': '/em-ha',
            'value': 'EMHA root node',
        },

        # 该节点代表了每个不同的 MySQL 集群节点
        'mysql_clusters': {
            'path': '/em-ha/mysql-clusters',
            'value': 'save many mysql cluster node',
        },

        # 管理节点进行选举的时候使用的节点
        'mgr_leader_election': {
            'path': '/em-ha/mgr-leader-election',
            'value': 'EMHA manager switch leader lock',
        },

        # Agent 节点进行选角leader的时候使用的节点
        'agent_leader_election': {
            'path': '/em-ha/agent-leader-election',
            'value': 'EMHA agent switch leader lock',
        },

        # 这个节点中保存了每个 MySQL 集群节点，优先切换的机房
        'priority_machine_room': {
            'path': '/em-ha/priority-machine-room',
            'value': 'When master switch, where room switch',
        },

        # Agent 开始做任务时, 会在这个节点中创建节点。代表当前agent正在工作
        'agent_working': {
            'path': '/em-ha/agent-working',
            'value': 'agent working',
        },

        # 有在该节点下的 MySQL 集群。代表停止主从切换
        'stop_failover': {
            'path': '/em-ha/stop-failover',
            'value': 'stop mysql failover',
        },

        # 当前管理节点的 Leader 是谁
        'mgr_leader': {
            'path': '/em-ha/mgr-leader',
            'value': 'tag current manager leader',
        },

        # 当前 MySQL 集群的 Agent Leader 是谁
        'agent_leader': {
            'path': '/em-ha/agent-leader',
            'value': 'tag current agent leader',
        },

        # 当前所有的管理节点, 是临时节点
        'manager': {
            'path': '/em-ha/manager',
            'value': 'save current all manager node',
        },

        # 当前cluster的 Master 是谁
        'cluster_master': {
            'path': '/em-ha/cluster-master',
            'value': 'tag current cluster master',
        },

        # 修改cluster数据的时候需要进行资源锁
        'update_cluster_data_lock': {
            'path': '/em-ha/update-cluster-data-lock',
            'value': 'tag current cluster master',
        },

        # 没有个Agent需要处理的队列
        'agent_queue': {
            'path': '/em-ha/agent-queue',
            'value': 'agent queue',
        },
    }
