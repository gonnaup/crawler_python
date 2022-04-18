import json
import random
import time

import psycopg2
import requests

from selenium_connect import selenium_doubanmovie, close_webdriver


class MovieData:
    # 电影数据模型
    def __init__(self, id, title, rate, directors, casts, url, star, cover, cover_x, cover_y, progress):
        self.id = id
        self.title = str(title).replace("'", '')
        self.rate = rate if rate else 0
        self.directors = ','.join(directors).replace("'", '_')
        self.casts = ','.join(casts).replace("'", '_')
        self.url = url
        self.star = star if rate else 0
        self.cover = cover
        self.cover_x = cover_x if rate else 0
        self.cover_y = cover_y if rate else 0
        self.progress = progress

    def sql(self) -> str:
        sql = f"insert into crawl_doubanmovie_data(id, title, rate, directors, casts, url, star, cover, cover_x, cover_y, progress) values " \
              f"({self.id}, '{self.title}', {self.rate}, '{self.directors}', '{self.casts}', '{self.url}', {self.star}, '{self.cover}', {self.cover_x}, {self.cover_y}, {self.progress})"
        return sql

    def __str__(self) -> str:
        return f"Movie(id={self.id}, title={self.title}, rate={self.rate})"


class CrawlDB:
    def __init__(self):
        # 数据库连接
        print("============= connect to postgresql db ================")
        self.CONN = psycopg2.connect(host="linuxserver.cn", port=5432, database="test", user="test", password="test")
        print("============= connect to postgresql success! =============")

    def fetch_progress(self) -> int:
        # 获取爬取进度
        ID = 1
        sql = f'select progress from crawl_doubanmovie_progress where id = {ID}'
        cur = self.CONN.cursor()
        cur.execute(sql)
        progress = cur.fetchone()
        return progress[0]

    def update_progress(self, progress):
        # 更新爬取进度
        ID = 1
        pre = self.fetch_progress()
        sql = f'update crawl_doubanmovie_progress set progress = {progress} where id = {ID}'
        cur = self.CONN.cursor()
        cur.execute(sql)
        print(f"更新爬取进度 {pre} ==> {progress}")
        self.CONN.commit()

    def add_progress_status(self, progress, status: bool):
        # 添加爬取状态
        stat = 'true' if status else 'false'
        sql = f'insert into crawl_doubanmovie_status(progress, status) values({progress}, {stat})'
        cur = self.CONN.cursor()
        cur.execute(sql)
        self.CONN.commit()

    def add_movie_data(self, movie_data: MovieData):
        sql = movie_data.sql()
        cur = self.CONN.cursor()
        cur.execute(sql)
        self.CONN.commit()

    def delete_movie_data_nlt_progress(self, progress):
        # 删除数据中 progress 大于等于 progress 的数据
        sql = f'delete from crawl_doubanmovie_data where progress >= {progress}'
        cur = self.CONN.cursor()
        cur.execute(sql)
        self.CONN.commit()

    def delete_movie_status_nlt_progress(self, progress):
        # 删除 progress 大于等于 progress 的状态
        sql = f'delete from crawl_doubanmovie_status where progress >= {progress}'
        cur = self.CONN.cursor()
        cur.execute(sql)
        self.CONN.commit()

    def close(self):
        self.CONN.close()
        print("=========== close the connection of progresql ================")


HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36 Edg/100.0.1185.39",
    "Accept": "text/html,application/json,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cookie": 'viewed="5387403"; bid=xqA4UUMHBjk; gr_user_id=e24f6d96-ff2a-45d5-ad07-cddf179b2fde; ll="118275"; ap_v=0,6.0; push_noty_num=0; push_doumail_num=0; ct=y',
    "Perpose": "just learning",
    "Connection": "keep-alive",
    "Host": "movie.douban.com",
    "Peferer": "https://movie.douban.com/tag/",
    "Pragma": "no-cache",
    "sec-ch-ua-mobile": '?0',
    "Sec-Fetch-Dest": 'document',
    'Sec-Fetch-Site': 'none',
    "Sec-Fetch-User": '?1',
    "sec-ch-ua-platform": "Windows",
    "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="100", "Microsoft Edge";v="100"',
    "Cache-Control": "no-cache",
    "Upgrade-Insecure-Requests": '1',
    "Sec-Fetch-Mode": "cors"}

# 主url
URL = 'https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start='

# 每次获取数据数量
STEP = 20

# 最大爬取数
MAX = 10000

# 数据库操作
db = CrawlDB()


def crawl_douban_movie():
    progress = db.fetch_progress()
    print(f"============= 初始化爬取进度 ==>> 当前：{progress} 最大：{MAX}===============")
    db.delete_movie_data_nlt_progress(progress)
    print("================= 初始电影数据 ===================")
    db.delete_movie_status_nlt_progress(progress)
    print("================= 初始话爬取状态 ===================")

    print(">>>>>>>>>>>>>>>>>>> 开始爬取数据 >>>>>>>>>>>>>>>>>>>>")
    while progress <= MAX:
        data = obtainResource_selenum(progress)
        if data:
            if len(data.get('data')) > 0:
                # 解析数据
                movies = data.get('data')
                for movie in movies:
                    movie_d = MovieData(movie.get('id'), movie.get('title'), movie.get('rate'), movie.get('directors'),
                                        movie.get('casts'), movie.get('url'),
                                        movie.get('star'), movie.get('cover'), movie.get('cover_x'),
                                        movie.get('cover_y'), progress)
                    db.add_movie_data(movie_d)
                # 成功添加电影数据后添加状态
                print("电影数据成功存入数据库")
                db.add_progress_status(progress, True)
            else:
                print(
                    f"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx 爬取 start = {progress} 数据为空，结束爬取任务 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
                break
        progress += STEP
        # 更新progress
        db.update_progress(progress)
    db.close()


# 数据流结束 {"data":[]}
# IP异常 {"msg":"....", "r":1}
#
# 正常数据 {"data": [{}, {}, ...}
# 异常返回码 {} '!,\x01\x04{"msg":"检测到有异常请求从您的IP发出，请登录再试!","r":1}\x03'


def obtainResource(progress: int) -> dict:
    # 重试
    url = URL + str(progress)
    count = 0
    first = 20  # 初次ip异常时暂停时间
    while True:
        r = requests.get(url, headers=HEADERS)
        if r.status_code == 200:
            # 去除{}边的异常字符
            text = r.text[r.text.index('{'): r.text.rindex('}') + 1]
            d = json.loads(text)
            result_data = dict(d)
            # 正常获取数据
            if result_data.get('data'):
                print(f"成功获取进度 {progress} 的数据 {len(result_data.get('data'))}条")
                r.close()
                time.sleep(random.randint(5, 20))
                return result_data
            elif result_data.get('msg'):
                print(f"IP被检查异常 ==> {result_data.get('msg')}")
                r.close()
                time.sleep(first * (count + 1))
                count += 1
        else:
            print(f"状态码异常 => {r.status_code}")
            print("准备长时间暂停爬取")
            db.add_progress_status(progress, False)
            r.close()
            time.sleep(30)
            return {}


def obtainResource_selenum(progress: int) -> dict:
    # 使用selenium 爬取
    url = URL + str(progress)
    count = 0
    first = 10  # 初次ip异常时暂停时间
    while True:
        result_data = selenium_doubanmovie(url)
        # 正常获取数据
        if result_data.get('data'):
            print(f"成功获取进度 {progress} 的数据 {len(result_data.get('data'))}条")
            time.sleep(random.randint(5, 20))
            return result_data
        elif result_data.get('msg'):
            print(f"IP被检查异常 ==> {result_data.get('msg')}")
            time.sleep(first * (count + 1))
            count += 1
        else:
            close_webdriver() # 关闭浏览器
            raise Exception(f'错误的数据格式 ==> {result_data}')


if __name__ == '__main__':
    # 单独爬取json数据会被禁IP
    crawl_douban_movie()
    pass
