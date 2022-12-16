from datetime import date
from time import sleep

from bs4 import BeautifulSoup, Tag
from peewee import *
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

from framework import *

PROGRESS_NAME = 'douban_novel'


class Novel(Model):
    """
    豆瓣小说数据模型
    """
    id = IntegerField(primary_key=True)  # ID
    title = CharField()  # title
    author = CharField()  # author name
    authorUrl = CharField()  # author page url
    kind = CharField(null=True)  # kind
    words = CharField(null=True)  # 字数
    status = CharField(null=True)  # 状态
    tag = CharField(null=True)  # 标签
    introduce = CharField(max_length=2000)  # introduce
    novelUrl = CharField(max_length=500)
    coverUrl = CharField(max_length=500)

    def __str__(self):
        return f'Novel[id={self.id}, title={self.title}, author={self.author}, kind={self.kind}, ' \
               f'words={self.words}, status={self.status}, tag={self.tag}]'

    class Meta:
        database = DB
        table_name = 't_douban_novel'


class NovelNodeLoader(Loader):
    current_page = 0

    def __init__(self):
        self._driver = edge_webdriver()
        # first page
        self._driver.get('https://read.douban.com/category/?sort=hot&sale=&page=1&progress=')

        # 小说表
        Novel.create_table()

        # 进度表
        Progress.create_table()
        p: Progress = Progress.get_or_none(Progress.name == PROGRESS_NAME)
        today = date.today()
        if p:
            print(f'数据库中存在 progress = {PROGRESS_NAME} => {p}')
            p.date = today
            p.save()
            self._progress = p
        else:
            self._progress = Progress.create(name=PROGRESS_NAME, progress=0, date=today)
            print(f'数据库中不存在 Progress = {PROGRESS_NAME} 初始化数据 => {self._progress}')

        # 初始化最大页数
        self._wait_until_page_loaded()
        max_page_node = self._driver.find_element(By.XPATH, '//*[@id="react-root"]/div/section[2]/div[2]/ul/li[7]/a')
        self.max_page = int(max_page_node.text.strip())
        print(f'最大页数为 {self.max_page} ...')
        # 初始化进度
        print(f'初始化当前进度为 {self._progress.progress} ...')
        self.current_page = self._progress.progress
        print('爬取进度初始化完毕，开始爬取数据 ############')

    def load_next(self):
        if (pg := self.current_page + 1) <= self.max_page:
            nex_url = f'https://read.douban.com/category/?sort=hot&sale=&page={pg}&progress='
            print(f'MAX[{self.max_page}] => 加载第 {pg} 页数据 ...')
            self._driver.get(nex_url)
            self._wait_until_page_loaded()
            retry = 3  # 当遇到节点未加载完毕时，重试次数
            for i in range(retry):
                try:
                    if (cur := self._find_cur_page()) != pg:
                        raise Exception(f'页码加载错误 目标 {pg}， 当前 {cur}')
                    soup = BeautifulSoup(self._driver.page_source, 'lxml')
                    novels_root = soup.find('div', id='react-root').find('ul', class_='works-list')
                    if not novels_root:
                        raise Exception('page not loaded completely')
                    self.current_page += 1
                    return novels_root
                except Exception as e:
                    if i >= retry - 1:
                        print('重试失败...')
                        raise Exception(e)
                    print(f'加载当前页码 {pg} 出现异常 => {e}')
                    sleep(i + 1)
                    print(f'@@@开始第 {i + 1} 次重试')
        else:
            print(f'已经达到最大页数 {self.max_page}, 停止爬取 ******')
            print(f'总共爬取 {Novel.select().count()} 条数据')
            self._progress.progress = 0
            self._progress.save()
            print('进度清零完毕...')
            return None

    def update_progress(self):
        self._progress.progress += 1
        return self._progress.save()

    def _find_cur_page(self):
        retry = 3  # 当遇到节点未加载完毕时，重试次数
        for i in range(retry):
            try:
                page_active = self._driver.find_element(By.CLASS_NAME, 'paginator-full') \
                    .find_element(By.CSS_SELECTOR, '[class="page active"]')
                # .find_element(By.CSS_SELECTOR, '.page .active')
                return int(page_active.text.strip())
            except Exception as e:
                if i >= retry - 1:
                    print('重试失败...')
                    raise Exception(e)
                print(f'寻找当前页码出现异常 => {e}')
                sleep(i + 1)
                print(f'@@@开始第 {i + 1} 次重试')

    def _wait_until_page_loaded(self):
        # until()接收一个参数，类型为函数，唯一参数为 webdriver， 返回值为 bool
        WebDriverWait(self._driver, timeout=20).until(self.__wait_until_condition)

    @staticmethod
    def __wait_until_condition(driver):
        ul = driver.find_element(
            By.ID, 'react-root').find_element(By.CLASS_NAME, 'works-list')
        return (ul.find_elements(By.CLASS_NAME, 'works-item') and not ul.find_elements(By.CLASS_NAME,
                                                                                       'is-loading'))

    def __next_url(self):
        return f'https://read.douban.com/category/?sort=hot&sale=&page={self.current_page}&progress='

    def __del__(self):
        self._driver.quit()


class NovelNodeParser(Parser):
    base_url = 'https://read.douban.com/'

    def parse_node(self, root: Tag) -> list[Novel]:
        li_list = root.find_all('li')
        novels = []
        for li in li_list:
            try:
                data = self._parse_each_node(li)
                novels.append(data)
            except Exception as e:
                print(f"解析数据失败 {e}")
        return novels

    def _parse_each_node(self, node: Tag) -> Novel:
        novel = Novel()
        id_content = node.attrs['to']  # /column/60817682/
        _id = id_content[8:len(id_content) - 1]
        novel.id = _id

        root_div = node.find('div')

        info_div = root_div.find('div', class_='info')
        title_ = info_div.find('h4', class_='title')
        novel.title = title_.find(
            'a', class_='title-container').attrs['title']
        author_ = info_div.find('div', class_='author')
        author_a = author_.find('a')
        novel.authorUrl = self.base_url + author_a.attrs['href']
        novel.author = author_a.next.next.text.strip()
        intro_ = info_div.find('div', class_='intro')
        novel.introduce = ''.join(
            intro_.next.next.text.strip().split()) if intro_.next.next else ""
        etra_info_ = info_div.find('div', class_='extra-info')
        sticky_info_ = etra_info_.find('div', class_='sticky-info')
        sticky_info_a = sticky_info_.find('a', class_='kind-link')
        novel.kind = sticky_info_a.text.strip()
        sticky_info_span = sticky_info_.find('span', class_='')
        novel.words = sticky_info_span.text.strip() if sticky_info_span else 0
        flexible_info_ = info_div.find(
            'div', class_='flexible-info flexible-hide-disabled')
        flexible_info_span_list = flexible_info_.find_all(
            'span', class_='flexible-info-item')
        for span in flexible_info_span_list:
            span_a = span.find('a', class_='tag')
            if span_a:
                novel.tag = span_a.text.strip()
            else:
                novel.status = span.text.strip()
        if not novel.status:
            novel.status = '已完结'

        cover_div = root_div.find('div', class_='cover shadow-cover')
        novel.novelUrl = self.base_url + cover_div.next.attrs['href']
        img_ = cover_div.find('img')
        novel.coverUrl = img_.attrs['src']
        return novel


class NovelsHandler(Handler):

    def hande(self, data: list[Novel]):
        update_count = 0
        create_count = 0
        for novel in data:
            if Novel.get_or_none(Novel.id == novel.id):
                # 存在则更新
                update_count += novel.save()
            else:
                # 不存在则添加
                create_count += novel.save(force_insert=True)
        print(f'解析数据 {len(data)} 条，更新 {update_count} 条，添加 {create_count} 条 ~~~~~~~~~')


def start_crawle_novel():
    engine = CrawleEngine(NovelNodeLoader(), NovelNodeParser(), NovelsHandler())
    engine.engine_start(stop_max=3)


if __name__ == '__main__':
    start_crawle_novel()
