# coding: utf-8
'''
Created on 2016年8月8日

@author: linary
'''

from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState, KazooState
import json
from load_balance.ServerData import ServerData

class Proxy(object):
    '''
    代理类，内部维护所有工作服务器的信息，并监听/server的子节点列表变化
    当有新的服务器加入时，其内部维护的工作服务器列表增加一个元素
    当有服务器宕机时，将该服务器本身维持的连接分配给其他的服务器
    '''

    def __init__(self, zk_server_address, server_path):
        '''
        Constructor
        '''
        # 服务器父节点路径
        self.server_path = server_path
        # 登记的正在工作的服务器
        self.servers = []
        
        self.zkclient = KazooClient(zk_server_address)
        # 添加连接状态监听器
        self.zkclient.add_listener(self.connect_listener)
        # 添加对服务器父节点的子节点列表的监听
        self.zkclient.ChildrenWatch(self.server_path, self.servers_change)
        
    
    # 连接状态监听器
    def connect_listener(self, state):
        if state == KeeperState.CONNECTED:
            print "代理已经开启..."
        elif state == KazooState.LOST:
            print "代理停止服务..."
        else:
            raise Exception("代理未正常开启...")
    
    
    # 子节点列表的监听器
    def servers_change(self, children):
        # 更新子节点列表，这个列表是由机器id组成的列表
        # 要能够通过机器id映射得到机器实例
        self.servers = children
              
    
    def start(self):
        self.zkclient.start()
    
    
    def stop(self):
        self.zkclient.stop()
        self.zkclient.close()
        
    
    # 得到每台服务器的基本信息，包括了负载
    def get_server_datas(self):
        server_datas = []
        # 遍历所有的子节点
        for server in self.servers:
            # 得到每台工作服务器的负载信息，这个是一直在动态变化的，是否需要控制一下呢？
            node_value = self.zkclient.get(self.server_path + "/" + server)
            # 这里得到的是序列化的服务器数据
            server_data_str = node_value[0]
            # 这里将字符串反序列化
            server_data = json.loads(server_data_str, object_hook = ServerData.unserialize)
            server_datas.append(server_data)
        return server_datas
            
    
    # 均衡策略
    def balance_load(self, server_datas):
        '''
        返回负载最低的服务器
        '''
        # 根据load排序
        server_datas.sort(None, key = lambda server_data: server_data.load)
        # 返回
        return server_datas[0]
        