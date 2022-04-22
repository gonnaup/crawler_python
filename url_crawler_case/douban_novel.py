import json
import sys
import time
from typing import Any

import pika
from bs4 import BeautifulSoup, Tag
from pika import BasicProperties
from pika.exchange_type import ExchangeType
import psycopg2
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from url_crawler import BaseUrlItem, BaseUrlParser, BaseDataHandler, DefualtCrawlerEngine


class NovelProgress:

    def __init__(self) -> None:
        print('init database connecting...')
        self.__connection = psycopg2.connect(
            host="linuxserver.cn", port=5432, database="test", user="test", password="test")
        print('database connecting successful...')

    def fetch_progress(self) -> int:
        # 获取爬取进度
        ID = 1
        sql = f'select progress from crawl_doubannovel_progress where id = {ID}'
        cur = self.__connection.cursor()
        cur.execute(sql)
        progress = cur.fetchone()
        return progress[0]

    def update_progress(self, progress: int):
        # 更新爬取进度
        ID = 1
        pre = self.fetch_progress()
        sql = f'update crawl_doubannovel_progress set progress = {progress} where id = {ID}'
        cur = self.__connection.cursor()
        cur.execute(sql)
        print(f"更新爬取进度 {pre} ==> {progress}")
        self.__connection.commit()

    def __del__(self):
        self.__connection.close()
        print('数据库连接已关闭')


db = NovelProgress()


class DoubanNovelUrlItem(BaseUrlItem):

    def __init__(self, max_page: int, current_page=-1):
        self.current_page = 0
        # 手动指定当前爬取页
        if current_page > 0:
            self.current_page = current_page
            db.update_progress(self.current_page)
        else:
            self.current_page = db.fetch_progress()
            print(f'初始化起始爬取页为 [{self.current_page}]')
        self.MAX_PAGE = max_page
        if self.current_page >= self.MAX_PAGE:
            print(
                f'WARN: current_page[{self.current_page} >= max_page[{self.MAX_PAGE}!')
            sys.exit(0)

    def current_progress(self) -> int:
        return self.current_page

    def __next__(self) -> str:
        if self.current_page > self.MAX_PAGE:
            raise StopIteration
        nex_url = f'https://read.douban.com/category/?sort=hot&sale=&page={self.current_page}&progress='
        self.current_page += 1
        return nex_url

    def __iter__(self):
        return self


class DoubanNovelSeleniumUrlParser(BaseUrlParser):

    def __init__(self, retry_count: int = 2):
        """
        param retry_count: 超时重试次数
        """
        self.base_url = 'https://read.douban.com/'
        self.retry_count = retry_count
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()))

    def parse_url(self, url) -> Any:
        retry = 0
        while retry < self.retry_count:
            try:
                print(f'开始获取页面 [{url}] 数据...')
                self.driver.get(url)
                # 等待加载到数据节点为止
                WebDriverWait(driver=self.driver, timeout=5).until(
                    self.__wait_until)
                soup = BeautifulSoup(self.driver.page_source, 'lxml')
            except TimeoutException as e:
                print(f'获取页面 [{url}] 超时')
                print(f'错误信息 ==> {e.msg}')
                time.sleep(1)
                retry += 1
                if retry < self.retry_count:
                    print(f"开始尝试第 [{retry}] 次重新获取...")
                continue
            data = self._parse_data(soup)
            return data
        print(f'页面 [{url}] 获取失败')
        return {}

    def __wait_until(self, driver: WebDriver):
        """
        等待条件
        :param driver:
        :return:
        """
        ul = driver.find_element(
            By.ID, 'react-root').find_element(By.CLASS_NAME, 'works-list')
        return ul.find_elements(By.CLASS_NAME, 'works-item') and not ul.find_elements(By.CLASS_NAME,
                                                                                      'is-loading')

    def _parse_data(self, _soup: BeautifulSoup) -> list:
        react_root = _soup.find('div', id='react-root')
        # 数据表集合<ul/>
        ul = react_root.find('ul', class_='works-list')
        li_list = ul.find_all('li')
        data_list = []
        for li in li_list:
            try:
                data = self._parse_node(li)
                data_list.append(data)
            except Exception as e:
                print(f"解析数据失败 {e}")
        return data_list

    def _parse_node(self, root: Tag) -> dict:
        data = {}
        id_content = root.attrs['to']  # /column/60817682/
        _id = id_content[8:len(id_content) - 1]
        data['id'] = _id

        root_div = root.find('div')

        info_div = root_div.find('div', class_='info')
        title_ = info_div.find('h4', class_='title')
        data['title'] = title_.find(
            'a', class_='title-container').attrs['title']
        author_ = info_div.find('div', class_='author')
        author_a = author_.find('a')
        data['authorUrl'] = self.base_url + author_a.attrs['href']
        data['author'] = author_a.next.next.text.strip().replace(
            "'", "_") if author_a.next.next else ""
        intro_ = info_div.find('div', class_='intro')
        data['introduce'] = ''.join(
            intro_.next.next.text.strip().split()) if intro_.next.next else ""
        etra_info_ = info_div.find('div', class_='extra-info')
        sticky_info_ = etra_info_.find('div', class_='sticky-info')
        sticky_info_a = sticky_info_.find('a', class_='kind-link')
        data['kind'] = sticky_info_a.text.strip()
        sticky_info_span = sticky_info_.find('span', class_='')
        data['words'] = sticky_info_span.text.strip() if sticky_info_span else 0
        flexible_info_ = info_div.find(
            'div', class_='flexible-info flexible-hide-disabled')
        flexible_info_span_list = flexible_info_.find_all(
            'span', class_='flexible-info-item')
        for span in flexible_info_span_list:
            span_a = span.find('a', class_='tag')
            if span_a:
                data['tag'] = span_a.text.strip()
            else:
                data['status'] = span.text.strip()
        if not data.get('status'):
            data['status'] = '已完结'

        cover_div = root_div.find('div', class_='cover shadow-cover')
        data['novelUrl'] = self.base_url + cover_div.next.attrs['href']
        img_ = cover_div.find('img')
        data['coverUrl'] = img_.attrs['src']
        return data

    def __del__(self):
        self.driver.close()
        print('shutdown webdriver complete...')


class RabbitDoubanNovelDataHandler(BaseDataHandler):
    """
    将数据发送到rabbitMQ中
    """

    def __init__(self, queue_name, exchange_name, routing_key):
        self._queue_name = queue_name
        self._exchange_name = exchange_name
        self._routing_key = routing_key
        credential = pika.PlainCredentials('test', 'test')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('linuxserver.cn', port=5672, virtual_host='/', credentials=credential))
        self.__channal = connection.channel()
        print('rabbitmq connecting successful...')
        self.__channal.exchange_declare(exchange=self._exchange_name, exchange_type=ExchangeType.topic.name,
                                        durable=True)

    def handle(self, data: Any):
        for _data in data:
            d = json.dumps(_data, ensure_ascii=False)
            self.__channal.basic_publish(exchange=self._exchange_name, routing_key=self._routing_key,
                                         body=d.encode(encoding='utf-8'),
                                         properties=BasicProperties(content_type='application/json',
                                                                    headers={
                                                                        '__TypeId__': 'novel'}))  # 指定类型，便于java的json转换器转换成指定的java类
        print(f"发送小说数据 {len(data)} 条到rabbitmq...")


class DoubanNovelCrawlerEngine(DefualtCrawlerEngine):

    def _before_handle_data(self, data, progress: int) -> bool:
        # 未爬取到数据，直接退出爬虫，本例中一般为页面结束
        if not data:
            print(f'WARN: 第{progress}页未爬取到数据，可能是数据页已结束，请确认!!!')
            sys.exit(0)
        return True

    def _after_handle_data(self, data, progress: int):
        super()._after_handle_data(data, progress)
        # 更新数据库进度
        db.update_progress(progress)


if __name__ == '__main__':
    """
    参数处理:
    -m 最大爬取页面数，默认3280
    -s 开始爬取的页面，默认从数据库读取上次爬取进度
    -h 显示参数帮助，只能在第一个参数才生效
    """
    args = sys.argv
    max_page = 3280
    start_page = -1
    if len(args) > 1:
        params = args[1:]
        if params[0] == '-h':
            print('-m 最大爬取页面数，默认3280')
            print('-s 开始爬取的页面，默认从数据库读取上次爬取进度')
            print('-h 显示参数帮助')
            sys.exit(0)
        p_names = set(['-m', '-s'])
        for i, p in enumerate(params):
            # 偶数位为参数位
            if (i & 1) == 0:
                if p in p_names:
                    if p == '-m':
                        try:
                            max_page = int(params[i+1])
                            print(f'最大爬取页数设置为 {max_page}')
                            continue
                        except:
                            print('-m 所带参数必须是数字')
                            sys.exit(-1)
                    if p == '-s':
                        try:
                            start_page = int(params[i+1])
                            print(f'起始爬取页设置为 {start_page}')
                            continue
                        except:
                            print('-s 所带参数必须是数字')
                            sys.exit(-1)
                    continue
                print(f'参数[{p}]不支持，请使用"-h"查看支持的参数')
                sys.exit(-1)
    else:
        print(f'未设置参数，使用默认值, 起始爬取页为 {start_page}, 最大爬取页数设置为 {max_page}')
        print(f'要设置参数，请使用 -h 查看')
    _queue_name = '_crawler_douban_novel'
    _exchange_name = '_exchange_crawler'
    _routing_key = 'crawler.douban.novel'
    _data_handler = RabbitDoubanNovelDataHandler(
        queue_name=_queue_name, exchange_name=_exchange_name, routing_key=_routing_key)
    engine = DoubanNovelCrawlerEngine(url_item=DoubanNovelUrlItem(max_page=max_page, current_page=start_page), url_parser=DoubanNovelSeleniumUrlParser(),
                                      data_handler=_data_handler)
    engine.start_crawl()
