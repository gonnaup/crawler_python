from configparser import ConfigParser

""" 使用标准配置解析库解析数据库配置文件db.ini """
"""
ini文件以 '.ini', '.cfg', '.conf'结尾，由节、键、值组成。
节：[section]
参数（键=值）
注释使用分号（;）
例如：
; last modified 1 April 2001 by John Doe  [owner]  name=John Doe  organization=Acme Products
[database]
server=192.0.2.42     ; use IP address in case network name resolution is not working
port=143
file=acme payroll.dat
"""
config = ConfigParser()
config.read('db.ini', encoding='UTF-8')
db_config = config['DB']
db_type = db_config.get('type')
database = db_config.get('database')
host = db_config.get('host')
user = db_config.get('user')
password = db_config.get('password')
""" 条件导入 """
_DB = None
if db_type.lower() == 'mysql':
    from peewee import MySQLDatabase

    _DB = MySQLDatabase(database, host=host, user=user, password=password)
elif db_type.lower() == 'postgres':
    from peewee import PostgresqlDatabase

    _DB = PostgresqlDatabase(database, host=host, user=user, password=password)
else:
    raise NameError(f'不支持 {db_type} 类型的数据库，请选择 mysql 或 postgres !')
print(f"{db_type.upper()} database init finished... ", 'OK!' if _DB.connect() else 'FAILED!')
