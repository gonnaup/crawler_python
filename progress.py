from peewee import Model, AutoField, CharField, IntegerField, TimestampField


class Progress(Model):
    id = AutoField()
    name = CharField(unique=True)
    progress = IntegerField()
    datetime = TimestampField()

    def __str__(self):
        return f'[id={self.id}, name={self.name}, progress={self.progress}, datetime={self.datetime}]'

    class Meta:
        table_name = 't_crawler_progress'
