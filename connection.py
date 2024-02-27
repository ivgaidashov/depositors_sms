import cx_Oracle
import time
import utils
import os
from conf import oracle_ip, oracle_database, oracle_password, oracle_username

class Database(object):
   connection = None

   def __init__(self):
       while Database.connection is None:
           try:
               utils.log_info('Подключение к БД')
               dsn_tns = cx_Oracle.makedsn(oracle_ip, '1521', service_name=oracle_database)
               Database.connection = cx_Oracle.connect(user=oracle_username, password=oracle_password, dsn=dsn_tns)
               if Database.connection is None:
                   time.sleep(5)
           except cx_Oracle.Error as error:
               utils.log_error('Ошибка подключения к БД: ' + str(error))

   def close(self, upload_batch_guid):
       utils.log_info('Закрытие соединения с БД')
       Database.connection.close()