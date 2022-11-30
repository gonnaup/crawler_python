import random
from abc import abstractmethod
from time import sleep


class NodeLoader:
    """
    dom 的数据跟节点加载器
    """

    @abstractmethod
    def load_next(self):
        """
        加载下一个待解析的dom节点
        :return
        """
        pass

    @abstractmethod
    def update_progress(self):
        pass


class NodeParser:
    """
    数据跟节点解析器
    """

    @abstractmethod
    def parse_node(self, node) -> []:
        """
        解析dome节点对象为数据对象
        :return:
        """
        pass


class DomainHandler:
    """
    数据对象处理器
    """

    @abstractmethod
    def hande(self, data: []):
        """
        数据对象处理
        :param data: 数据对象
        """
        pass


class CommonCrawleEngine:
    def __init__(self, pageLoader: NodeLoader, nodeParser: NodeParser, handler: DomainHandler):
        self.loader = pageLoader
        self.parser = nodeParser
        self.handler = handler

    def engin_start(self, stop_min: int = 1, stop_max: int = 8):
        # 解析出 dom 的数据跟节点
        node = self.loader.load_next()
        while node:
            # 解析出数据 list
            data = self.parser.parse_node(node)
            # 处理数据
            self.handler.hande(data)
            # 更新进度
            self.loader.update_progress()
            # 等待
            sleepTime = random.randint(stop_min, stop_max)
            sleep(sleepTime)
            # 下一节点
            node = self.loader.load_next()
