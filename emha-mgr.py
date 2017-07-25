#!/usr/bin/env python
#-*- coding:utf-8 -*-

from emha.emha_manager import EMHAManager
import argparse
import threading
import time
import sys
import os
import signal
import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

def kill_sign_op(signum, frame):
    """当接收到kill 信号执行关闭流打印输出 和"""
    print signum
    print frame
    print "process killed {pid}...".format(pid = os.getpid())
    raise Exception("catch kill signal")

def parse_args():
    """解析命令行传入参数"""
    usage = """
Usage Example: nohup python emha-mgr.py --zk-hosts='10.10.10.1:2181,192.168.137.11:2181,127.0.0.1:2181' > /tmp/emha-mgr_$(date +%F).log 2>&1 &

Description:
    EMHA Manager site. what to do:
    1.it register manager node in zookeeper.
    2.listen Manager node change.
    3.election Manager become Leader.
    4.listen MySQL Cluster and MySQL instance change.
    5.put message to per MySQL Cluster queue.
    """

    # 创建解析对象并传入描述
    parser = argparse.ArgumentParser(description = usage, 
                            formatter_class = argparse.RawTextHelpFormatter)

    # 添加 MySQL Host 参数
    parser.add_argument('--zk-hosts', dest='zk_hosts', required = True,
                      action='store', default='127.0.0.1:2181',
                      help='Connection Zookeeper Cluster hosts', metavar='host:port')

    args = parser.parse_args()

    return args

def do_queue():
    """循环操作 Manager queue"""

    global emha_mgr

    emha_mgr.init_mgr_queue()
    emha_mgr.do_queue()

# 全局变量
emha_mgr = EMHAManager()

def main():
    # 注册 捕获型号kill信号
    signal.signal(signal.SIGINT, kill_sign_op) # 终止进程 中断进程 (control+c)
    signal.signal(signal.SIGTERM, kill_sign_op) # 终止进程 软件终止信号

    args = parse_args() # 解析传入参数

    try:
        # 链接 Zookeeper
        emha_mgr.conn(hosts = args.zk_hosts)

        # 初始化节点
        emha_mgr.init_nodes()

        # 监听MySQL集群节点
        emha_mgr.watch_mysql_clusters_children()

        # 注册 Manager
        emha_mgr.register()

        # 选举 Leader
        emha_mgr.election()

        # 对leader节点进行监听
        emha_mgr.leader_watch_children()

        # 开启线程处理 Manager queue 队列
        mgr_queue_t = threading.Thread(target=do_queue)
        mgr_queue_t.start()

        # TODO
        # 启动一个 socket 可以进行接收命令
        print 'main end open socket will todo'

        """
        while True:
            mgr_queue_t.join(2)
            if not mgr_queue_t.isAlive:
                break
        """

        while True:
            time.sleep(2)

        # mgr_queue_t.join()
    except KeyboardInterrupt:
        print "Ctrl-c pressed ..."
    except Exception as e:
        print traceback.format_exc()
    finally: # 需要断开zk 链接
        emha_mgr.disconn()
        print 'sys exit'
        os._exit(1)

if __name__ == '__main__':
    main()
