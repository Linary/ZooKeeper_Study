# coding: utf-8
'''
分布式共享锁
说明：
    又称读锁，某个事务对数据对象加上了共享锁之后，只能对他进行读取操作，其他事务
    也只能对该数据对象进行读操作。主要逻辑是：
    1）首先工作服务器启动时在zookeeper的/shared_lock节点下创建一个顺序临时
    子节点，并添加对/shared_lock节点的子节点变更监听，可以把自己的工作模式写
    到节点名上或是写到节点的数据域内。
    2）获取/shared_lock节点下的所有子节点，确定自己的节点序号在所有子节点中
    的顺序。
    3）获取锁：对于读请求，如果没有比自己序号小的子节点，或是所有比自己小的子节
    点都是读请求，那么可以获取共享锁，然后开始执行自己的业务逻辑；如果比自己小的
    节点有写请求，则向比自己小的最后一个写请求节点注册监听，然后等待事件触发。对
    于自身是写请求的节点，如果自己的序号是最小，那么获取锁，开始执行业务逻辑；否
    则，就向比自己序号小的最后一个节点注册监听，然后等待事件触发。
    4）接收到Watcher通知后，重复步骤2）
问题：
    1）最好的实现方式应该是：封装出一个XXX_Mutex类，然后在主函数中调用该类的
    加锁/解锁方法，控制各个工作服务器的运行顺序
扩展：
'''

import logging
from time import sleep
from shared_lock.Server import Server

# 日志设置为默认配置
logging.basicConfig()


if __name__ == "__main__":
    # 一些常量
    # zookeeper服务器的地址
    ZOOKEEPER_SERVER = "127.0.0.1:2181"
    # 共享锁根节点路径
    LOCK_PARENT_PATH = "/shared_lock"
    # 工作服务器的数量
    SERVER_NUM = 5
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
    
    