from datetime import date
from time import sleep

from bs4 import BeautifulSoup, Tag
from peewee import Model, DateField, CharField, DecimalField, AutoField
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

from framework import *

PROGRESS_NAME = 'tonghuashun_blocktrade'


class BlockTrade(Model):
    """
    同花顺大宗交易数据模型
    """
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
        database = DB
        table_name = 't_tonghuashun_blocktrade'


class BlockTradesNodeLoader(Loader):
    _current_page = 1
    _url = 'http://data.10jqka.com.cn/market/dzjy/'
    _first_load = True

    def __init__(self):
        driver = chrome_webdriver()
        self.__driver = driver
        self.__driver.implicitly_wait(5)
        self.__driver.get(self._url)

        # 如果表不存在则创建
        Progress.create_table()
        p: Progress = Progress.get_or_none(Progress.name == PROGRESS_NAME)

        # 创建 大宗交易表
        BlockTrade.create_table()

        self.__wait_page_load_completed()
        # 初始化最大页数
        el_page_info = self.__driver.find_element(By.XPATH, '//*[@id="J-ajax-main"]/div[2]/span')
        _page_info = el_page_info.text  # 1/200
        self.max_page = int(_page_info.removeprefix('1/'))
        print(f'max page {self.max_page}')

        today = date.today()
        # 进度初始化
        if p:
            print(f'数据库中存在 progress = {PROGRESS_NAME} => {p}')
            # 如果当天还未爬取数据，但进度不为0
            # 说明在以前有未完成的爬取
            # 直接进度清零
            if p.date < today and p.progress != 0:
                print(f'检测到 {p.date} 未爬取完毕的进度 {p.progress}，清零此进度...')
                p.progress = 0
                p.save()
            # 更新日期
            if p.date != today:
                print(f'更新进度日期 {p.date} 为当天 {today}')
                p.date = today
                p.save()
            self._progress = p
            # 初始化页面进度
            self.__init_current_page_to_db_progress()

            #  当前页码 = 进度 + 1
            assert self._current_page == self._progress.progress + 1
        else:
            self._progress = Progress.create(name=PROGRESS_NAME, progress=0, date=today)
            print(f'数据库中不存在 Progress = {PROGRESS_NAME} 初始化数据 => {self._progress}')

        # 数据初始化
        if self._progress.progress == 0:
            # 进度为0则说明当天未爬取
            # 页面数据为最近3个月数据，直接删除最近3个月数据后重新填充
            three_month_ago = plus_month(today, -3)
            BlockTrade.delete().where(BlockTrade.trade_date.between(three_month_ago, today)).execute()
            print(f'删除最近3个月 [{three_month_ago}] - [{today}] 的大宗交易数据...')

        print('爬取进度初始化完毕，开始爬取数据 ############')

    def load_next(self):
        if self._current_page < self.max_page:
            if not self._first_load:
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
            self._first_load = False
            """
            BeautifulSoup:
            find()、findAll()：只查找子节点中的元素
            findNext()、findAllNext()：查找子节点和节点后的所有节点中的元素
            """
            return btfs.find('div', id='J-ajax-main').find('table', class_='m-table J-ajax-table').find('tbody')
        else:
            print(f'已经达到最大页数 {self.max_page}, 停止爬取 ******')
            print(f'总共爬取 {BlockTrade.select().count()} 条数据')
            self._progress.progress = 0
            self._progress.save()
            print('进度清零完毕...')
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

                page_a_number_els = self.__find_page_els()
                pagination_max_page = page_a_number_els[len(page_a_number_els) - 1]
                # 当前最大可点击页码
                cur_max_page_num = int(pagination_max_page.text.strip())
                # 当前最大页码小于目标页码
                if cur_max_page_num < target_page:
                    # 最后一页有点击bug
                    if cur_max_page_num < self.max_page - 10:
                        # 点击最大页码
                        pagination_max_page.click()
                        print(f'点击页码 {cur_max_page_num}')
                        self.__wait_page_load_completed()
                        if (p := self.__find_cur_page()) != cur_max_page_num:
                            sleep(3)
                            if self.__find_cur_page() != cur_max_page_num:
                                print(f'点击页码 {cur_max_page_num} 失败，当前页 {p}')

                    # 点击下一页
                    self.__find_next_button().click()
                    self.__wait_page_load_completed()
                else:
                    for p in page_a_number_els:
                        if target_page == int(p.text.strip()):
                            print(f'找到初始化页面 {target_page}')
                            p.click()
                            self.__wait_page_load_completed()
                            wait_count = 0
                            while self.__find_cur_page() != target_page:
                                if wait_count > 10:
                                    print(f'多次等待后尝试重新点击 {target_page} ye')
                                    p.click()
                                print(f'@@@等待 {target_page} 页加载完毕')
                                sleep(1)
                                wait_count += 1
                            break

            self._current_page = _cur_page
            assert self._current_page == self._progress.progress + 1
            print(f'页面初始化到 [{self._current_page}] 完毕 ...')

    def __find_page_els(self):
        retry = 1
        # 所有数字页码 el
        while True:
            try:
                page_a_els = self.__driver.find_element(By.XPATH, '//*[@id="J-ajax-main"]/div[2]').find_elements(
                    By.TAG_NAME, 'a')
                page_a_number_els = list(filter(lambda el: str(el.text).strip().isnumeric(), page_a_els))
                # 按页号大小排序
                page_a_number_els.sort(key=lambda x: int(x.text))
                for p in page_a_number_els:
                    # just check the element loaded
                    str(p.text)
                return page_a_number_els
            except Exception as e:
                if retry > 3:
                    raise Exception(e)
                sleep(retry)
                retry += 1

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


class BlockTradesNodePaser(Parser):
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
                print(f'************ 当前页第 {i + 1} 行数据格式错误, 正常每行列数 {row_length}，当前 {len(fields)} 列')
        print(f'解析当前页面共获取 {len(trades)} 条正常数据 >>>>>')
        return trades


class BlockTradesHandler(Handler):

    def hande(self, data: list[BlockTrade]):
        data_fields = list(map(lambda d: d.__dict__['__data__'], data))
        # postgres 返回插入数据的 ID 集合
        results = BlockTrade.insert_many(data_fields).execute()
        # mysql 返回数据总条数（int），postgres 返回插入数据集合
        print(f'接收数据 {len(data)} 条，存入数据库 {results if isinstance(results, int)  else len(list(results))} 条 ~~~~~~~~~')


def start_crawle_blockTrade():
    engine = CrawleEngine(BlockTradesNodeLoader(), BlockTradesNodePaser(), BlockTradesHandler())
    engine.engine_start(min_pause=3, max_pause=6)


if __name__ == '__main__':
    start_crawle_blockTrade()
