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
    def parse_node(self, root) -> []:
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

    def engine_start(self, min_pause=1, max_pause=5):
        # 解析出 dom 的数据跟节点
        root = self.loader.load_next()
        while root:
            # 解析出数据 list
            data = self.parser.parse_node(root)
            # 处理数据
            self.handler.hande(data)
            # 更新进度
            self.loader.update_progress()
            # 等待
            sleepTime = random.randint(min_pause, max_pause)
            sleep(sleepTime)
            # 下一节点
            root = self.loader.load_next()
