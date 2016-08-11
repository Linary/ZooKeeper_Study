# coding: utf-8
'''
Created on 2016年8月11日

@author: linary
'''

from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState, KazooState, EventType
import threading
import string
from time import sleep


class Server(threading.Thread):
    '''
    工作服务器（也是ZooKeeper的客户端）
    '''
    # 控制输出信息的锁，注意：这个是单机器的锁，这里实现的是分布式锁，并不存在本末倒置
    print_mutex = threading.Lock()
    
    DELAY_TIME = 3
    
    def __init__(self, zk_server_address, lock_base_path, host, serve_mode):
        threading.Thread.__init__(self)
        # 锁的根节点路径
        self.lock_base_path = lock_base_path
        # 主机IP
        self.host = host
        # 工作模式，读/写
        self.serve_mode = serve_mode
        # 事件，初始化为False
        self.event = threading.Event()
        
        # 创建一个zookeeper客户端
        self.zkclient = KazooClient(zk_server_address)
        # 添加连接状态监听器
        self.zkclient.add_listener(self.zk_connect_listener)
        # 与zookeeper开启连接
        self.zkclient.start()
        
    
    # 连接状态监听器
    def zk_connect_listener(self, state):
        # 获取打印锁
        Server.print_mutex.acquire()
        if state == KeeperState.CONNECTED:
            print self.host + " 已经开启..."
        elif state == KazooState.LOST:
            print self.host + " 停止服务..."
        else:
            raise Exception(self.host + " 未正常开启...")   
        # 获取打印锁
        Server.print_mutex.release() 
      
        
    # 初始化
    def run(self):
        # 创建锁节点，形如/shared_lock/192.168.0.0-R-0000000001
        self.create_lock_node()
        # 获取锁
        self.acquire_lock()
        # 工作
        self.work()
        # 释放锁
        self.release_lock()
        # 准备停止
        self.stop()
        
        
    def create_lock_node(self):
        # 先检查父节点，如果父节点不存在
        if not self.zkclient.exists(self.lock_base_path):
            # 先创建父节点
            self.zkclient.create(self.lock_base_path)
        # 拼凑出服务器子节点的完整路径
        node_path = self.lock_base_path + "/" + self.host + "-" + self.serve_mode + "-"
        # 创建临时顺序节点
        self.node_path = self.zkclient.create(node_path, "", self.zkclient.default_acl, True, True)
    
    
    # 删除事件的响应
    def pre_node_delete_watch(self, data, stat, event):
        if event and event.type == EventType.DELETED:
            # 将事件设置为True
            self.event.set()
    
    
    # 获取锁
    def acquire_lock(self):
        # 提取出自己的节点名
        node_name = self.node_path.split("/")[-1]
        # 获取/shared_lock子节点排序列表
        sorted_children = self.get_sorted_children()
        # 得到节点的索引
        node_index = sorted_children.index(node_name)
        
        # 判断自己是不是序号最小的节点
        if node_index == 0:
            # 立马返回，占用锁
            return
        # 如果不是，向比自己小的最后一个节点注册监听
        else:
            # 拼凑出前一个节点的路径
            pre_node_path = self.lock_base_path + "/" + sorted_children[node_index - 1]
            # 添加对前一个节点的删除事件的关注
            self.zkclient.DataWatch(pre_node_path, self.pre_node_delete_watch)
            # 这里应该等待锁
            self.event.wait()
            
    
    def work(self):
        # 获取打印锁
        Server.print_mutex.acquire()
        # 如果是写请求，
        if self.serve_mode == "W":
            # 写一会数据，然后删除节点，关闭会话
            print self.host + " 正在写数据..."
        else:
            # 读一会数据，然后删除节点，关闭会话
            print self.host + " 正在读数据..."
        Server.print_mutex.release()
        # 这里暂停几秒钟。模拟工作耗时状态
        sleep(self.DELAY_TIME)
    
    
    # 释放锁
    def release_lock(self):
        # 删除自己的节点
        self.zkclient.delete(self.node_path)
    
    
    # 获取/shared_lock子节点排序列表
    def get_sorted_children(self):
        # 获取/shared_lock子节点列表
        children = self.zkclient.get_children(self.lock_base_path)
        ###############################################################
        # 这里sort函数的比较表达式是由两个函数实现，还挺有技巧的
        ###############################################################
        # 返回节点的序列号
        def get_lock_node_seq(node_name):
            # 分割字符串，然后返回列表最后一个元素，先将其转化为整型
            return string.atoi(node_name.split("-")[-1])
        # 编号比较r函数
        def sequence_compare(node1, node2):
            return get_lock_node_seq(node1) - get_lock_node_seq(node2)
        # 将列表排序
        children.sort(cmp = sequence_compare)
        
        return children
        
    
    # 停止工作
    def stop(self):
        # 移除事件监听器
        self.zkclient.remove_listener(self.pre_node_delete_watch)
        # 会话
        self.zkclient.stop()
        self.zkclient.close()    
    