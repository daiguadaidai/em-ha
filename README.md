# em-ha
基于 MySQL5.7无损半同步和Zookeeper的HA

Action 编号代表的意义
    1: 代表有MySQL有集群节点有增删操作。具体是增加还是删除不懂.(mysql-clusters/project)下面有节点变动
        data = {
            'action': 1,
            'nodes': ['node1', 'node2', 'node3']
        }
    2: 代表有MySQL有集群有新节点加入
    3: 代表有MySQL有集群有节点退出
    11: 代表需要初始化MySQL集群的相关节点. 一般是在MySQL集群第一次加入EMHA中就会有这个动作
        data = {
            'action': 11,
            'node_name': 'project Name',
        }

Agent Node Name: `project:room:typ:host:port`

集群节点中数据的结构:

```
cluster_data = {
    'machine_rooms': {
        'room_1': {
            'master': 'instance1'
            'slave': [instance2, 'instance3],
            'dt': [instance2, 'instance3],
            'delay': [instance2, 'instance3],
        },
        'room_2': {
            'master': 'instance1'
            'slave': [instance2, 'instance3],
            'dt': [instance2, 'instance3],
            'delay': [instance2, 'instance3],
        },
    },
}
```

多活类型:

1. normal: 普通集群
2. drc: DRC 多活
3. gz: Gloabl Zone 多活
