import json
import time
from typing import Any

import pika
from bs4 import BeautifulSoup, Tag
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from url_crawler import BaseUrlItem, BaseUrlParser, BaseDataHandler, DefualtCrawlerEngine


class DoubanNovelUrlItem(BaseUrlItem):

    def __init__(self, max_page: int):
        self.current_page = 1
        self.MAX_PAGE = max_page

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
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    def parse_url(self, url) -> Any:
        retry = 0
        while retry < self.retry_count:
            try:
                self.driver.get(url)
                # 等待加载到数据节点为止
                WebDriverWait(driver=self.driver, timeout=5).until(self.__wait_until)
                soup = BeautifulSoup(self.driver.page_source, 'lxml')
            except TimeoutException as e:
                print(f'获取页面 {url} 超时')
                print(f'错误信息 ==> {e.msg}')
                time.sleep(5)
                continue
            data = self._parse_data(soup)
            return data
        print(f'页面 {url} 获取失败')

    def __wait_until(self, driver: WebDriver):
        """
        等待条件
        :param driver:
        :return:
        """
        ul = driver.find_element(By.ID, 'react-root').find_element(By.CLASS_NAME, 'works-list')
        return ul.find_elements(By.CLASS_NAME, 'works-item') and not ul.find_elements(By.CLASS_NAME,
                                                                                      'is-loading')

    def _parse_data(self, _soup: BeautifulSoup) -> [{}]:
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

    def _parse_node(self, root: Tag) -> {}:
        data = {}
        id_content = root.attrs['to']  # /column/60817682/
        _id = id_content[8:len(id_content) - 1]
        data['id'] = _id

        root_div = root.find('div')

        info_div = root_div.find('div', class_='info')
        title_ = info_div.find('h4', class_='title')
        data['title'] = title_.find('a', class_='title-container').attrs['title']
        author_ = info_div.find('div', class_='author')
        author_a = author_.find('a')
        data['authorUrl'] = self.base_url + author_a.attrs['href']
        data['author'] = author_a.next.next.text.strip().replace("'", "_") if author_a.next.next else ""
        intro_ = info_div.find('div', class_='intro')
        data['introduce'] = ''.join(intro_.next.next.text.strip().split()) if intro_.next.next else ""
        etra_info_ = info_div.find('div', class_='extra-info')
        sticky_info_ = etra_info_.find('div', class_='sticky-info')
        sticky_info_a = sticky_info_.find('a', class_='kind-link')
        data['kind'] = sticky_info_a.text.strip()
        sticky_info_span = sticky_info_.find('span', class_='')
        data['words'] = sticky_info_span.text.strip() if sticky_info_span else 0
        flexible_info_ = info_div.find('div', class_='flexible-info flexible-hide-disabled')
        flexible_info_span_list = flexible_info_.find_all('span', class_='flexible-info-item')
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

    def __init__(self, queue_name, exchange_name):
        self._queue_name = queue_name
        self._exchange_name = exchange_name
        credential = pika.PlainCredentials('test', 'test')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('linuxserver.cn', port=5672, virtual_host='/', credentials=credential))
        self.__channal = connection.channel()
        # self.__channal.queue_declare(queue=self._queue_name)

    def handle(self, data: Any):
        d = json.dumps(data, ensure_ascii=False)
        # self.__channal.basic_publish(exchange='', routing_key=self._queue_name, body=d.encode(encoding='utf-8'))
        print(d)


class DoubanNovelCrawlerEngine(DefualtCrawlerEngine):

    def _after_handle_data(self, data, progress: int):
        super()._after_handle_data(data, progress)
        time.sleep(1)


if __name__ == '__main__':
    _queue_name = '_crawler_douban_novel'
    _exchange_name = '_exchange_crawler'
    engine = DoubanNovelCrawlerEngine(url_item=DoubanNovelUrlItem(1), url_parser=DoubanNovelSeleniumUrlParser(),
                                      data_handler=RabbitDoubanNovelDataHandler(_queue_name, _exchange_name))
    engine.start_crawl()
