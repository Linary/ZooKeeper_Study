# coding: utf-8
'''
Created on 2016年8月8日

@author: linary
'''

class ServerData():
    '''
    '''
    
    def __init__(self, id_, name, host, port, load = 0):
        # 编号
        self.id = id_
        # 服务器名称
        self.name = name
        # ip地址
        self.host = host
        # 端口
        self.port = port
        # 服务器负载
        self.load = load
    
    
    # 序列化
    @staticmethod
    def serialize(server_data):
        return {
            'id': server_data.id,
            'name': server_data.name,
            'host': server_data.host,
            'port': server_data.port,
            'load': server_data.load}
    
    
    # 反序列化
    @staticmethod
    def unserialize(dict_):
        return ServerData(dict_['id'], 
                          dict_['name'], 
                          dict_['host'], 
                          dict_['port'], 
                          dict_['load']) 
    
