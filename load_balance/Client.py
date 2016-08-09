# coding: utf-8
'''
Created on 2016年8月8日

@author: linary
'''
from _socket import socket, AF_INET, SOCK_STREAM
import threading


class Client(threading.Thread):
    '''
    客户端类，经过Proxy与服务器建立连接
    '''

    def __init__(self, host, name):
        # 基类初始化
        threading.Thread.__init__(self)
        self.host = host
        self.name = name

    
    # 线程核心函数
    def run(self):
        self.connect()
    
    
    def stop(self):
        # 关闭套接字
        self.tcpClientSock.close()

    
    # 设置代理
    def set_proxy(self, proxy):
        self.proxy = proxy
        
    
    # 客户端发起连接请求
    def connect(self):
        # 得到负载最小的服务器
        server_data = self.proxy.balance_load(self.proxy.get_server_datas())
        # 从服务器信息中提取出ip和端口，拼成地址
        ADDR = (server_data.host, server_data.port)
#         BUFSIZE = 1024
        # 建立套接字
        self.tcpClientSock = socket(AF_INET, SOCK_STREAM)
        self.tcpClientSock.connect(ADDR)

        # 这里就不用循环发送消息了,直接发送一句话就好了
        self.tcpClientSock.send(self.name + " connected %d" % server_data.port)
        
#         while True:
#             data = raw_input(">")
#             if not data:
#                 break
#             # 发送数据
#             self.tcpClientSock.send(data)
#             # 接受数据
#             data = self.tcpClientSock.recv(BUFSIZE)
#             if not data:
#                 break
#             print data
    