from typing import Any


class BaseUrlItem:
    # url生成器

    def next(self) -> str:
        # 返回下一个url
        raise Exception('unsuport method!')

    def __getitem__(self, item) -> str:
        # 重载此方法实现类的可迭代化
        raise Exception('unsuport method!')


class BaseUrlParser:
    # url解析器
    def parse_url(self, url):
        raise Exception('unsuport method!')


class BaseDataHandler:
    # 数据处理器
    def handle(self, data: Any):
        raise Exception('unsuport method!')


class BaseCrawlerEngine:
    # 爬虫引擎
    def __init__(self, url_item: BaseUrlItem, url_parser: BaseUrlParser, data_handler: BaseDataHandler):
        self.url_item = url_item
        self.url_parser = url_parser
        self.data_handler = data_handler

    def start_crawler_engine(self):
        self.before_crawl()
        for url in self.url_item:
            self.before_parse_url()
            data = self.url_parser.parse_url(url)
            self.before_handle_data(data)
            self.data_handler.handle(data)

    def before_crawl(self):
        pass

    def before_parse_url(self):
        pass

    def before_handle_data(self, data):
        pass
