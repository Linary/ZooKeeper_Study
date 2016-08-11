# coding: utf-8
'''
分布式队列
说明：
问题：
扩展：
'''

import logging
from time import sleep
from distributed_queue.Server import Server

# 日志设置为默认配置
logging.basicConfig()


if __name__ == "__main__":
    # 一些常量
    # zookeeper服务器的地址
    ZOOKEEPER_SERVER = "127.0.0.1:2181"
    # 共享锁根节点路径
    LOCK_PARENT_PATH = "/queue_fifo"
    # 工作服务器的数量
    SERVER_NUM = 3
    # 工作服务器的工作模式
    SERVE_MODE = ["R", "W", "R", "R", "W"]
    
    
    # 工作服务器列表
    servers = []
    for i in range(SERVER_NUM):
        # 创建一台服务器
        server = Server(ZOOKEEPER_SERVER, LOCK_PARENT_PATH, "192.168.0.%d" % i, SERVE_MODE[i])
        # 服务器开始工作
        server.start()
        # 添加进列表
        servers.append(server)
        
        
    # 这里可以把两个循环合并成一个的
    for i in range(SERVER_NUM):
        # 关闭服务器
        servers[i].join()
    
    