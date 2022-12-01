from peewee import Model, AutoField, CharField, IntegerField, DateField

from datasource import DB_POSTGRES


class Progress(Model):
    id = AutoField()
    name = CharField(unique=True)
    progress = IntegerField()
    date = DateField()

    def __str__(self):
        return f'[id={self.id}, name={self.name}, progress={self.progress}, date={self.date}]'

    class Meta:
        database = DB_POSTGRES
        table_name = 't_crawler_progress'
