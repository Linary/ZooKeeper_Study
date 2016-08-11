# coding: utf-8
'''
命名服务
说明：
问题：
扩展：
'''

import logging
from time import sleep
from name_service.IDGenerator import IDGenerator

# 日志设置为默认配置
logging.basicConfig()


if __name__ == "__main__":
    # 一些常量
    # zookeeper服务器的地址
    ZOOKEEPER_SERVER = "127.0.0.1:2181"
    # 共享锁根节点路径
    Name_PARENT_PATH = "/name_service"
    # 工作服务器的数量
    SERVER_NUM = 5
    
    # 创建一个命名服务器
    id_generator = IDGenerator(ZOOKEEPER_SERVER, Name_PARENT_PATH)
    id_generator.start()
    
    for i in range(SERVER_NUM):
        # 创建一台服务器
        id_ = id_generator.generate_id()
        print "服务器 " + str(id_) + " 命名成功..." 
                
    id_generator.stop()    
