from peewee import PostgresqlDatabase

DB_POSTGRES = PostgresqlDatabase('test', host='localhost', user='postgres', password='123456')
print("database init finished... ", 'OK!' if DB_POSTGRES.connect() else 'FAILED!')
