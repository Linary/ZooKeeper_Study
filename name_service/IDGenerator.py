# coding: utf-8
'''
Created on 2016年8月11日

@author: linary
'''
from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState, KazooState
import threading

class IDGenerator(object):
    '''
    ID生成器
    '''
    
    # 控制输出信息的锁
    print_mutex = threading.Lock()
    DELAY_TIME = 3
    
    def __init__(self, zk_server_address, id_base_path):
        self.id_base_path = id_base_path
        
        # 创建一个zookeeper客户端
        self.zkclient = KazooClient(zk_server_address)
        # 添加连接状态监听器
        self.zkclient.add_listener(self.zk_connect_listener)
        # 与zookeeper开启连接
        self.zkclient.start()
        
    
    # 连接状态监听器
    def zk_connect_listener(self, state):
        # 获取打印锁
        IDGenerator.print_mutex.acquire()
        if state == KeeperState.CONNECTED:
            print "命名服务器已经开启..."
        elif state == KazooState.LOST:
            print "命名服务器停止服务..."
        else:
            raise Exception("命名服务器未正常开启...")   
        # 获取打印锁
        IDGenerator.print_mutex.release()     
        
        
    
    def start(self):
        # 创建父节点
        self.create_name_node()
    
    
    def create_name_node(self):
        # 先检查父节点，如果父节点不存在
        if not self.zkclient.exists(self.id_base_path):
            # 先创建父节点
            self.zkclient.create(self.id_base_path)
    
    
    # 生成id
    def generate_id(self):
        # 拼凑出服务器子节点的完整路径
        node_path = self.id_base_path + "/"
        # 创建临时顺序节点
        self.node_path = self.zkclient.create(node_path, "", self.zkclient.default_acl, True, True)
        
        # 准备删除创建出来的节点
        self.zkclient.delete(self.node_path)
            
        return self.node_path.split("/")[-1]
    
    
    # 停止服务
    def stop(self):
        self.zkclient.stop()
        self.zkclient.close()
    
    