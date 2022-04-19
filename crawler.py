from abc import abstractmethod
from collections.abc import Iterator
from typing import Any


class BaseUrlItem(Iterator):
    """
    URL迭代器，持续提供需要爬取的url，需要子类实现方法
    """

    @abstractmethod
    def current_progress(self) -> int:
        """
        当前爬取进度
        :return:
        """
        return 0

    @abstractmethod
    def __next__(self):
        raise Exception('method not support!')

    @abstractmethod
    def __iter__(self):
        raise Exception('method not support!')


class BaseUrlParser:
    """
    url解析器，解析url返回所需数据
    """

    @abstractmethod
    def parse_url(self, url) -> Any:
        raise Exception('method not support!')


class BaseDataHandler:
    """
    数据处理器，对数据进行展示或存储等操作
    """

    @abstractmethod
    def handle(self, data: Any):
        print(data)


class DefualtExcutor:
    """
    默认爬虫执行器，单线程执行，可实现多线程爬取，
    但无IP代理情况下建议放缓爬取速度
    """

    def __init__(self):
        pass

    @abstractmethod
    def excute(self, task, *args, **kwargs):
        task(*args, **kwargs)


class DefualtCrawlerEngine:
    # 爬虫引擎
    def __init__(self, url_item: BaseUrlItem, url_parser: BaseUrlParser, data_handler: BaseDataHandler,
                 excutor=DefualtExcutor()):
        self.url_item = url_item
        self.url_parser = url_parser
        self.data_handler = data_handler
        self.excutor = excutor

    def start_crawl(self):
        self.__start_crawler_engine()

    def __start_crawler_engine(self):
        """
        通用爬取过程逻辑
        """
        self._before_crawl()
        for url in self.url_item:
            self.excutor.excute(self.__crawl_item(url))

    def __crawl_item(self, url):
        progress = self.url_item.current_progress()
        self._before_parse_url(progress)
        data = self.url_parser.parse_url(url)
        if self._before_handle_data(data, progress):
            self.data_handler.handle(data)
            self._after_handle_data(data, progress)

    def _before_crawl(self):
        """
        爬取前执行方法
        """
        pass

    def _before_parse_url(self, progress: int):
        """
        解析url之前执行方法
        """
        pass

    def _before_handle_data(self, data, progress: int) -> bool:
        """
        处理数据之前执行方法，可用作数据校验和装饰
        :param progress:
        :param data: 数据
        :return 是否执行数据处理
        """
        return True

    def _after_handle_data(self, data, progress: int):
        """
        处理数据之后执行方法
        :param progress:
        :param data: 数据
        """
        pass
