from datetime import datetime, date

from bs4 import BeautifulSoup, Tag
from peewee import Model, PostgresqlDatabase, DateField, CharField, DecimalField, AutoField
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.microsoft import EdgeChromiumDriverManager

from engine import *
from progress import Progress

PROGRESS_NAME = 'tonghuashun_blocktrade'
db = PostgresqlDatabase('test', host='localhost', user='postgres', password='123456')
print("database init finished... ", 'OK!' if db.connect() else 'FAILED!')


class BlockTrade(Model):
    id = AutoField(primary_key=True)  # ID
    trade_date = DateField()  # 交易时间
    stock_code = CharField()  # 股票代码
    stock_url = CharField()  # 股票url
    stock_name = CharField()  # 股票名称
    price_latest = DecimalField(decimal_places=2)  # 最新价
    price_trade = DecimalField(decimal_places=2)  # 成交价
    volume_10000 = DecimalField(decimal_places=2)  # 成交量（万股）
    premium_rate = CharField()  # 溢价率
    department_buyer = CharField()  # 买房营业部
    department_seller = CharField()  # 卖方营业部

    def __str__(self):
        return f'BlockTrade[id={self.id}, trade_date={self.trade_date}, stock_code={self.stock_code}, ' \
               f'stock_name={self.stock_name}, latest_price={self.price_latest}, trade_price={self.price_trade}, ' \
               f'volume={self.volume_10000}, premium_rate={self.premium_rate}, department_buyer={self.department_buyer}, ' \
               f'department_seller={self.department_seller}] '

    class Meta:
        database = db


class BlockTradesNodeLoader(NodeLoader):
    _current_page = 1
    _url = 'http://data.10jqka.com.cn/market/dzjy/'

    def __init__(self):
        """
        webdrive init
        """
        options = webdriver.EdgeOptions()
        # 去除顶部栏的 "Microsoft Edge 正由自动测试软件控制" 字样
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        driver = webdriver.Edge(options=options, service=EdgeService(EdgeChromiumDriverManager().install()))
        # 设置 window.navigator.webdriver=undefined
        # 否则 使用 drive.get() 启动浏览器时，window.navigator.webdriver属性为true，
        # 反爬虫脚本会根据此属性判断为爬虫脚本
        script = '''
            Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
            })
            '''
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": script})
        self.__driver = driver
        self.__driver.get(self._url)
        # 绑定 db
        Progress.bind(database=db)
        # 如果表不存在则创建
        Progress.create_table()
        p: Progress = Progress.get_or_none(Progress.name == PROGRESS_NAME)
        self.__wait_page_load_completed()
        el_page_info = self.__driver.find_element(By.XPATH, '//*[@id="J-ajax-main"]/div[2]/span')
        _page_info = el_page_info.text  # 1/200
        self.max_page = int(_page_info.removeprefix('1/'))
        print(f'max page {self.max_page}')
        if p:
            print(f'数据库中存在 progress = {PROGRESS_NAME} => {p}')
            self._progress = p
            # 初始化页面进度
            self.__init_current_page_to_db_progress()

            #  当前页码 = 进度 + 1
            assert self._current_page == self._progress.progress + 1
        else:
            self._progress = Progress.create(name=PROGRESS_NAME, progress=0, datetime=datetime.now())
            print(f'数据库中不存在 Progress = {PROGRESS_NAME} 初始化数据 => {self._progress}')

        print('爬取进度初始化完毕，开始爬取数据 ############')

    def load_next(self):
        if self._current_page < self.max_page:
            if self._current_page > 1:
                print(f'开始翻页 [{self._current_page}] -> [{self._current_page + 1}] ...')
                self.__find_next_button().click()
                self.__wait_page_load_completed()
                self._current_page += 1
                # 页面未翻页成功 重试
                for i in range(1, 4):
                    if self._current_page != (p := self.__find_cur_page()):
                        print(f'@@@等待翻页[{p}] -> [{self._current_page}]中 第 {i} 次 ...')
                        sleep(i)
                    else:
                        break

            print(f'加载第 [{self._current_page}] 页数据节点...')
            assert self._current_page == self.__find_cur_page()
            btfs = BeautifulSoup(self.__driver.page_source, 'lxml')
            """
            BeautifulSoup:
            find()、findAll()：只查找子节点中的元素
            findNext()、findAllNext()：查找子节点和节点后的所有节点中的元素
            """
            return btfs.find('div', id='J-ajax-main').find('table', class_='m-table J-ajax-table').find('tbody')
        else:
            print(f'已经达到最大页数 {self.max_page}, 停止爬取 ******')
            return None

    def update_progress(self):
        """
        更新 progress
        :return:
        """
        self._progress.progress += 1
        return self._progress.save()

    def __find_next_button(self):
        """
        获取下一页按钮 element
        :return:
        """
        retry = 1
        while True:
            try:
                page_a_els = self.__driver.find_elements(By.CLASS_NAME, 'changePage')
                next_el = list(filter(lambda el: el.text == '下一页', page_a_els))
                assert len(next_el) == 1
                return next_el[0]
            except Exception as e:
                if retry > 3:
                    print(f'@@@查找 [下一页] 按钮时重试失败 ##########')
                    raise Exception(e)
                print(f'@@@查找 [下一页] 按钮时出现异常，开始第 {retry} 次等待重试...')
                sleep(retry)
                retry += 1

    def __init_current_page_to_db_progress(self):
        """
        初始化当前页面
        """
        if self._progress.progress > 0:
            target_page = self._progress.progress + 1
            print(f'开始初始化页面到页数 [{target_page}] ...')

            # 循环点击，直到 currentpage > progress
            while (_cur_page := self.__find_cur_page()) < target_page:
                print(f'当前页为 [{_cur_page}]')
                self.__find_next_button().click()
                self.__wait_page_load_completed()
            self._current_page = _cur_page
            print(f'页面初始化到 [{self._current_page}] 完毕 ...')

    def __find_cur_page(self) -> int:
        """
        获取当前页码
        :return 当前页号
        """
        retry = 3  # 当遇到节点未加载完毕时，重试次数
        for i in range(retry):
            try:
                pagination = self.__driver.find_element(By.XPATH, '//*[@id="J-ajax-main"]/div[2]')
                current_page_tag = pagination.find_element(By.CLASS_NAME, 'cur')
                return int(current_page_tag.text)
            except Exception as e:
                if i >= retry - 1:
                    print('重试失败...')
                    raise Exception(e)
                print(f'寻找当前页码出现异常 => {e}')
                sleep(i + 1)
                print(f'@@@开始第 {i + 1} 次重试')

    def __wait_page_load_completed(self):
        """
        等待页面加载完毕
        """
        WebDriverWait(self.__driver, timeout=20).until(
            lambda x: x.find_element(By.CLASS_NAME, 'page_info'))
        sleep(1)

    def __del__(self):
        self.__driver.quit()


class BlockTradesNodePaser(NodeParser):
    def parse_node(self, tbody_node: Tag) -> []:
        """
        解析表格，得到大宗交易数据
        """
        # find all rows
        print('开始解析节点数据...')
        rows = tbody_node.find_all('tr')
        trades = []
        row_length = 10
        for i, row in enumerate(rows):
            fields = row.find_all('td')
            if len(fields) == row_length:
                trade = BlockTrade()
                year, month, day = fields[1].text.strip().split('-')
                trade.trade_date = date(int(year), int(month), int(day))
                code_a = fields[2].a
                trade.stock_code = code_a.text.strip()
                trade.stock_url = code_a.attrs['href']
                trade.stock_name = fields[3].a.text.strip()
                try:
                    price_latest_str = fields[4].text.strip()
                    trade.price_latest = float(price_latest_str)
                except ValueError as e:
                    print(f'最新价格转换失败 => {e}，使用0代替')
                    trade.price_latest = 0
                try:
                    trade.price_trade = float(fields[5].text.strip())
                except ValueError as e:
                    print(f'成交价格转换失败 => {e}，使用0代替')
                    trade.price_trade = 0
                try:
                    trade.volume_10000 = float(fields[6].text.strip())
                except ValueError as e:
                    print(f'成交量（万股）转换失败 => {e}，使用0代替')
                    trade.volume_10000 = 0
                trade.premium_rate = fields[7].text.strip()
                trade.department_buyer = fields[8].text.strip()
                trade.department_seller = fields[9].text.strip()
                trades.append(trade)
            else:
                print(f"************ 当前页第 {i + 1} 行数据格式错误, 正常每行列数 {row_length}，当前 {len(fields)} 列")
        print(f'解析当前页面共获取 {len(trades)} 条正常数据 >>>>>')
        return trades


class BlockTradesHandler(DomainHandler):

    def hande(self, data: list[BlockTrade]):
        print(f'处理数据 {len(data)} 条 ~~~~~~~~~')


def start_crawle_blockTrade():
    engine = CommonCrawleEngine(BlockTradesNodeLoader(), BlockTradesNodePaser(), BlockTradesHandler())
    engine.engin_start(stop_max=20, stop_min=10)


if __name__ == '__main__':
    start_crawle_blockTrade()