from peewee import *

from framework import *


class Novel(Model):
    """
    豆瓣小说数据模型
    """
    id = AutoField()  # ID
    title = CharField()  # title
    author = CharField()  # author name
    authorUrl = CharField()  # author page url
    kind = CharField()  # kind
    words = CharField()  # 字数
    status = CharField()  # 状态
    tag = CharField()  # 标签
    introduce = CharField(max_length=2000)  # introduce
    novelUrl = CharField(max_length=500)

    class Meta:
        database = DB
        table_name = 't_douban_novel'
