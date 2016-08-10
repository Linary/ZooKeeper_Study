# coding: utf-8
'''
Created on 2016年8月10日

@author: linary
'''

from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState, KazooState, EventType
import threading
import string

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
        
        # 创建一个zookeeper客户端
        self.zkclient = KazooClient(zk_server_address)
        # 添加连接状态监听器
        self.zkclient.add_listener(self.zk_connect_listener)
        # 与zookeeper开启连接
        self.zkclient.start()
        
    
    # 连接状态监听器
    def zk_connect_listener(self, state):
        if state == KeeperState.CONNECTED:
            print self.host + " 已经开启..."
        elif state == KazooState.LOST:
            print self.host + " 停止服务..."
        else:
            raise Exception(self.host + " 未正常开启...")    
      
        
    # 初始化
    def run(self):
        # 创建锁节点，形如/shared_lock/192.168.0.0-R-0000000001
        node_path = self.create_lock_node()
        # 获取锁
        self.acquire_lock(node_path)
        
        
    def create_lock_node(self):
        # 先检查父节点，如果父节点不存在
        if not self.zkclient.exists(self.lock_base_path):
            # 先创建父节点
            self.zkclient.create(self.lock_base_path)
        # 拼凑出服务器子节点的完整路径
        node_path = self.lock_base_path + "/" + self.host + "-" + self.serve_mode + "-"
        # 创建临时顺序节点
        return self.zkclient.create(node_path, "", self.zkclient.default_acl, True, True)
    
    
    # 删除事件的响应
    def pre_node_delete_watch(self, data, stat, event):
        if event and event.type == EventType.DELETED:
            print data + "deleted..."
    
    
    # 获取锁
    def acquire_lock(self, node_path):
        # 获取/shared_lock子节点排序列表
        sorted_children = self.get_sorted_children()
        # 提取出节点名
        node_name = node_path.split("/")[-1]
        # 得到节点的索引
        node_index = sorted_children.index(node_name)
        
#         # 删除事件的响应
#         def pre_node_delete_watch(data, stat, event):
#             if event and event.type == EventType.DELETED:
#                 print data + "deleted..."
        
        # 如果是写请求，
        if self.serve_mode == "W":
            # 如果是，再判断自己是不是序号最小的节点
            if  node_index == 0:
                # 占用锁，开始写数据
                self.write()
            # 如果不是，向比自己小的最后一个节点注册监听
            else:
                # 拼凑出前一个节点的路径
                pre_node_path = self.lock_base_path + "/" + sorted_children[node_index - 1]
                # 添加对前一个节点的删除事件的关注
                self.zkclient.DataWatch(pre_node_path, self.pre_node_delete_watch)
        # 如果是读请求
        else:
            # 寻找最后一个写节点
            def get_last_write_node_index():
                # 逆向遍历
                for i in range(node_index)[::-1]:
                    # 工作模式是节点名中的第二个部分
                    serve_mode = sorted_children[i].split("/")[1]
                    # 只要找到一个写请求，则立刻返回
                    if serve_mode == "W":
                        return i
                # 如果全部都是读请求，则返回-1
                return -1
            
            # 得到所有比自己小的子节点中的最后一个写节点的下标
            last_write_node_index = get_last_write_node_index()
            # 判断以下两个条件是否成立
            # 1）没有比自己序号小的子节点
            # 2）或是所有比自己小的子节点都是读请求
            # 如果成立
            if node_index == 0 or last_write_node_index < 0:
                # 占用共享锁，开始读数据
                self.read()
            # 如果不成立，向比自己小的最后一个写节点注册监听
            else:
                # 拼凑出前一个节点的路径
                pre_node_path = self.lock_base_path + "/" + sorted_children[last_write_node_index]
                # 添加对前一个节点的删除事件的关注
                self.zkclient.DataWatch(pre_node_path, self.pre_node_delete_watch)
    
    
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
        
    
    # 读数据
    def read(self):
        # 读一会数据，然后删除节点，关闭会话
        Server.print_mutex.acquire()
        print self.host + " 正在写数据..."
        Server.print_mutex.release()
        threading.Timer(self.DELAY_TIME, self.stop).start()
    
    
    # 写数据
    def write(self):
        # 写一会数据，然后删除节点，关闭会话
        Server.print_mutex.acquire()
        print self.host + " 正在读数据..."
        Server.print_mutex.release()
        threading.Timer(self.DELAY_TIME, self.stop).start()
        
        
    # 停止工作
    def stop(self):
        self.zkclient.stop()
        self.zkclient.close()    
    