# coding: utf-8
'''
Created on 2016年8月8日

@author: linary
'''
from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState, KazooState
import threading
import json
from ServerData import *
from _socket import socket, AF_INET, SOCK_STREAM


class Server(object):
    '''
    工作服务器类
    '''
    # 互斥锁
    mutex = threading.Lock()
    print_mutex = threading.Lock()
    
    def __init__(self, zk_server_address, server_path, server_data):
        # 服务器父节点路径
        self.server_path = server_path
        # 服务器本身的信息
        self.server_data = server_data
        # 子线程列表
        self.work_threads = []
        
        self.zkclient = KazooClient(zk_server_address)
        # 添加连接状态监听器
        self.zkclient.add_listener(self.zk_connect_listener)
        
    
    # 连接状态监听器
    def zk_connect_listener(self, state):
        if state == KeeperState.CONNECTED:
            print self.server_data.name + "已经开启..."
        elif state == KazooState.LOST:
            print self.server_data.name + "停止服务..."
        else:
            raise Exception(self.server_data.name + "未正常开启...")
    
    
    # 开启服务
    def start(self):
        # 与zookeeper开启连接
        self.zkclient.start()
        
        # 先检查父节点，如果父节点不存在
        if not self.zkclient.exists(self.server_path):
            # 先创建父节点
            self.zkclient.create(self.server_path)
        # 拼凑出服务器子节点的完整路径
        node_path = self.server_path + "/id%d" % self.server_data.id
        # 创建节点
        self.zkclient.create(node_path, "", self.zkclient.default_acl, True, False)
        # 序列化
        server_data_str = json.dumps(self.server_data, default = ServerData.serialize)
        # 把自身的信息写到节点上
        self.zkclient.set(node_path, server_data_str)
        
        # 准备接受客户端连接
        self.accept_connect = True
        # 这里应该创建一个子线程，开启TCP连接，主线程用于处理与zookeeper相关的事务
        self.accept_thread = threading.Thread(target = self.accept_client, args=())
        # 开启子线程
        self.accept_thread.start()
        
        
    def stop(self):
        # 关闭所有工作子线程
        for thread in self.work_threads:
            # 这里不知道为什么有一个线程永远等不到
            thread.join(1)
        # 关闭监听的线程，貌似没法停止
        self.accept_connect = False
        self.accept_thread.join(2)
        # 关闭与zookeeper的会话
        self.zkclient.stop()
        self.zkclient.close()
 
 
    # 这里还需要写一个与客户端建立连接的函数，这里应该把节点上的数值加1    
    def accept_client(self):
        BUFSIZE = 1024
        ADDR = (self.server_data.host, self.server_data.port)
        
        self.tcpServerSock = socket(AF_INET, SOCK_STREAM)
        self.tcpServerSock.bind(ADDR)
        self.tcpServerSock.listen(10)
        
        while self.accept_connect:
            # 接受连接必须处于循环提内部，不然只能接受一个请求
            # 这里会阻塞啊，怎么让他停住呢？
            tcpClientSock, _ = self.tcpServerSock.accept()
            
            def communicate():
                # 读取数据不应该加锁，不然会很浪费资源
                data = tcpClientSock.recv(BUFSIZE)
                # 这里应该加锁，
                Server.print_mutex.acquire()
                print data
                # 释放锁
                Server.print_mutex.release()
                
            # 这里也应该创建一个子线程进行通信
            thread = threading.Thread(target = communicate, args = ())
            # 开启子线程，这个线程也应该在某处join
            thread.start()
            # 添加进子线程列表
            self.work_threads.append(thread)
            
            # 这里应该加锁
            Server.mutex.acquire()
            # 增加负载
            self.add_load()
            # 释放锁
            Server.mutex.release()
        
#         # 这里也不关闭，就保持长连接
#         while True:
#             print '等待客户端连接...'
#             tcpClientSock, _ = self.tcpServerSock.accept()
#             print '连接来自于:', ADDR
#             
#             def communicate():
#                 # 消息循环
#                 while True:
#                     data = tcpClientSock.recv(BUFSIZE)
#                     if not data:
#                         break
#                     tcpClientSock.send('[%s] %s' % (ctime(), data))
#                 tcpClientSock.close()
#                  
#             # 这里也应该创建一个子线程进行通信
#             thread = threading.Thread(target = communicate, args = ())
#             # 开启子线程
#             thread.start()
#             # 这里应该加锁
#             Server.mutex.acquire()
#             # 增加负载
#             self.add_load()
#             # 释放锁
#             Server.mutex.release()
        
        
    def release_client(self):
        # 服务器释放自身的TCP连接
        self.tcpServerSock.close()
        # 减少负载
        self.sub_load()
        
    
    # 给指定的服务器增加负载
    def add_load(self):
        # 不知道这里是否需要加锁之类的
        self.server_data.load += 1
        # 拼凑出服务器子节点的完整路径
        # 因为这里是由服务器自己完成的回写，并不需要考虑并发问题
        node_path = self.server_path + "/id%d" % self.server_data.id
        server_data_str = json.dumps(self.server_data, default = ServerData.serialize)
        self.zkclient.set(node_path, server_data_str)
    
    
    # 给指定的服务器减少负载
    def sub_load(self):
        # 不知道这里是否需要加锁之类的
        self.server_data.load -= 1
        # 拼凑出服务器子节点的完整路径
        # 因为这里是由服务器自己完成的回写，并不需要考虑并发问题
        node_path = self.server_path + "/id%d" % self.server_data.id
        server_data_str = json.dumps(self.server_data, default = ServerData.serialize)
        self.zkclient.set(node_path, server_data_str)
