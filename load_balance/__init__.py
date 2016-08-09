# coding: utf-8

'''
负载均衡
说明：
    以kazoo(一个python版的zookeeper包)实现的负载均衡，包含三种机器身份，分别是
    服务器、代理和客户端，其中代理先打开，并监听zookeeper上/server节点的子节点列表
    服务器的变化，一旦其发生变化，便立即更新其内部维护的服务器信息列表;并能在客户端向
    服务器发起连接的时候，根据每台服务器的负载，选择出负载最轻的服务器共客户端连接。而
    服务器本身在代理之后启动，启动时在zookeepr的/server下创建一个临时节点，并开启一
    个监听客户端连接的子线程，每接受到一个连接，就创建一个子线程与之通信，然后增加自身
    负载量，并写到对应的临时节点上；每断开一个客户端的连接，就销毁一个通信子线程，然后
    减少负载量，并写到对应的临时节点上。客户端最后启动，启动后手动设置代理（在真实应用
    中可能不需要这样做），然后发起连接请求，这时内部先通过代理选择出合适的服务器，然后
    再建立TCP连接与服务器通信。
问题：
    1）客户端在发起连接时有一些连接信息未打印出来。
    2）代理在读取节点的负载信息，可能同时服务器也正在往该节点更新数据，导致读取脏数据
    3）主线程的提示信息显示时间是用sleep()控制的，无法适应动态场景的需求
    4）主线程即使使用了exit()都无法正常的终止
扩展：
    1）尝试加入master，也就是说所有的worker都可以竞选master，然后让他扮演代理身份
    2）服务器与客户端使用非阻塞式IO接口通信。
'''



import logging
from load_balance.Proxy import Proxy
from load_balance.ServerData import ServerData
from load_balance.Server import Server
from load_balance.Client import Client
from time import sleep
import sys


logging.basicConfig()


if __name__ == "__main__":
    
    ZOOKEEPER_SERVER = "127.0.0.1:2181"
    SERVER_PATH = "/server"
    SERVER_NUM = 3
    CLIENT_NUM = 10

    # 创建一个代理
    proxy = Proxy(ZOOKEEPER_SERVER, SERVER_PATH)
    # 代理开始工作
    proxy.start()

    # 服务器列表
    servers = []
    
    # 先启动服务器
    for i in range(SERVER_NUM):
        # 服务器自身的信息
        server_data = ServerData(i, "[server %d]" % i, "127.0.0.1", 6000 + i, 0)
        # 创建一台服务器
        server = Server(ZOOKEEPER_SERVER, SERVER_PATH, server_data)
        # 服务器开始工作
        server.start()
        # 添加到列表中，方便最后关闭
        servers.append(server)
    
    
    # 客户端列表
    clients = []
    for i in range(CLIENT_NUM):
        # 创建一个客户端
        client = Client("192.128.0.%d" % i, "[client %d]" % i)
        # 设置代理（简单起见）
        # 这里应该是通过网络连接先访问到代理，而不是直接在这里设置代理
        client.set_proxy(proxy)
        # 客户端开启
        client.start()
        # 添加至列表，方便最后关闭
        clients.append(client)
    
    # 等一会儿，这里需要线程同步
    sleep(3)
    raw_input("按任意键结束...")
     
    # 关闭所有客户端
    for i in range(CLIENT_NUM):
        client.stop()
        
    proxy.stop()
    
    for i in range(SERVER_NUM):
        servers[i].stop()
    
    # 为什么都走到这里了还没有退出程序    
    sys.exit(0)