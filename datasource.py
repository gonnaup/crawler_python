from peewee import PostgresqlDatabase

DB = PostgresqlDatabase('test', host='localhost', user='postgres', password='123456')
print("database init finished... ", 'OK!' if DB.connect() else 'FAILED!')
