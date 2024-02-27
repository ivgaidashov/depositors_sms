import sys
import time
import utils
import random
import requests
from connection import Database
from tabulate import tabulate
from datetime import datetime
from utils import log_error, log_info, log_warn, send_email
from conf import login, pswd, serverurl, log_folder, headers
import re

conn = Database()
log_messages = []
error_list = []
def send_request(type, url, headers, data):
    if type == 'post':
        try:
            x = requests.post(url, headers=headers, data=data)
            return x
        except Exception as error:
            log_error('Ошибка отправки запроса ' + str(error))
            raise
    elif type == 'get':
        try:
            x = requests.get(url, headers=headers, params=data)
            return x
        except Exception as error:
            log_error('Ошибка отправки запроса ' + str(error))
            raise

def current_balance():
    url = serverurl+'Balance/balance.php'
    myobj = {'login': login, 'pass': pswd}

    headers = {"Content-Type": "application/x-www-form-urlencoded",}
    x = float(send_request('post',url, headers, myobj).text)
    log_info(f'Текущий баланс ' + str(x))

    if x <= 500:
        log_error(f'Баланс менее 500 рублей. Выполните пополнение')
        send_email(['ivgaide@domain.ru'], 'Ошибка отправки СМС-оповещений', 'Баланс менее 500 рублей: '+ str(x))
        raise


def send_bulk_sms(batch):

    url = serverurl+'Send/SendBulkPacket/'
    phone_data_header = '{"sms": ['
    phone_data_trail = ']}'

    batch_id = random.randint(1, 1000000)

    for src, one in enumerate(batch):
        row = f'{{"phone": "{one["CPHONE_NBR"]}", "text": "{one["CTEXT"]}"}}'
        if len(batch)-1 != src:
            row += ', '
        phone_data_header += row

    message = phone_data_header+phone_data_trail
    log_info('Ключ phone_data')
    log_info(message)

    myobj = {'login': login, 'pass': pswd, 'messageId': batch_id, 'sourceAddress': 'BankTKPB',
             'phone_data': message, 
             "name_deliver": "batch_test"
              }

    headers = {"Content-Type": "application/x-www-form-urlencoded",}
    log_info('Отправка запроса SendBulkPacket')

    x = send_request('post',url, headers, myobj)

    log_info('Получен список идентификаторов сообщений: ' + x.text)
    received_ids = x.text.strip('][').split(',')

    wait_time = 60
    log_info(f'Ожидаем {wait_time} секунд перед проверкой доставки сообщений')

    time.sleep(wait_time)
    check_sms_status(batch, received_ids, batch_id)

def check_sms_status(batch, received_ids, batch_id):
    url = serverurl+'State/'

    for src, one in enumerate(received_ids):
        payload = {'login': login, 'pass': pswd, 'messageId':one}
        r = send_request('get', url, headers, payload)
        log_info(f'Статус сообщения {one}')
        log_info(r.json())

        datesentstr = utils.timestamp_to_date(int(re.search(r'\((.*?)\)', r.json()['ReportedDateUtc']).group(1)))
        price = r.json()['Price']
        status = r.json()['State']
        status_desc = r.json()['StateDescription']
        batch[src]['DDATESENT'] = datesentstr
        batch[src]['IPRICE'] = price
        batch[src]['IMESSTATUS'] = status
        batch[src]['CMESSDESC'] = status_desc
        batch[src]['CMESSAGEID'] = one
        batch[src]['CBATCHID'] = batch_id

    log_messages.extend(batch)

def send_sms():
    sql = 'SELECT * FROM TABLE(gis.f_depositors_sms())'
    batch_size = 50
    try:
        with conn.connection.cursor() as cursor:
            # execute the SQL statement
            cursor.execute(sql)
            batch_count = 1
            while True:
                rows = cursor.fetchmany(batch_size)
                list = []
                if not rows:
                    break
                for row in rows:
                    if row[5] != 'error':
                        row = {'ICUSNUM': row[0], 'IQDGIDENT': row[1], 'IQDGNUM': row[2], 'DQDGEND': row[3], 'CBATCHID': None, 'CMESSAGEID': None, 'CTEXT': row[4], 'CPHONE_NBR': row[5], 'DDATESENT': None, 'IMESSTATUS': None, 'CMESSDESC': None, 'IPRICE': None, 'DBATCHDATE': datetime.now()}
                        list.append(row)
                    else:
                        row = {'ICUSNUM': row[0], 'IQDGIDENT': row[1], 'IQDGNUM': row[2], 'DQDGEND': row[3], 'CBATCHID': None, 'CMESSAGEID': None, 'CTEXT': row[4], 'CPHONE_NBR': row[5], 'DDATESENT': None, 'IMESSTATUS': None, 'CMESSDESC': None, 'IPRICE': None, 'DBATCHDATE': datetime.now()}
                        error_list.append(row)

                log_info(f"Пачка {batch_count}: \n {tabulate(list, headers='keys', tablefmt='psql')}")
                send_bulk_sms(list)
                batch_count += 1

    except Exception as error:
        log_error('Ошибка получения списка получателей '+ str(error))
        send_email(['ivgaide@domain.ru'], 'Ошибка отправки СМС-оповещений', 'Ошибка получения списка получателей '+ str(error))

def execute_many(data):
    log_info(f" \n {tabulate(data, headers='keys', tablefmt='psql')}")

    cols = ','.join(list(data[0].keys()))
    params = ','.join(':' + str(k) for k in list(data[0].keys()))
    statement = f"insert into gis_dep_sms ({cols}) values ({params})"
    try:
        with conn.connection.cursor() as cursor:
            cursor.executemany(statement, data, batcherrors=True)
            conn.connection.commit()
            for error in cursor.getbatcherrors():
                log_error('Ошибка ' + str(error.code) + ' ' + str(error.message))
                log_error(data[error.offset])
    except Exception as error:
        log_error('Ошибка сохранения ' + str(error))

def check_remaining_sms():
    sql = 'SELECT CMESSAGEID FROM gis_dep_sms where IMESSTATUS = -1'
    in_progress_list = []
    try:
        with conn.connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    log_info('Есть сообщения с неподтвержденным статусом:')
                    if row:
                        new_row = {'CMESSAGEID': row[0], 'IMESSTATUS': None, 'CMESSDESC': None }
                        log_info(new_row)
                        in_progress_list.append(new_row)
            else:
                log_info('Нет сообщений со статусом InProcess')
                sys.exit()

    except Exception as error:
        log_error('Ошибка получения списка сообщений со статусом InProcess '+ str(error))
        send_email(['ivgaide@domain.ru'], 'Ошибка обновления статусов СМС-оповещений', 'Ошибка получения списка сообщений со статусом InProcess '+ str(error))

    log_info('Отправка запроса')
    url = serverurl+'State/'
    for src, one in enumerate(in_progress_list):
        payload = {'login': login, 'pass': pswd, 'messageId': one['CMESSAGEID']}
        r = send_request('get', url, headers, payload)
        log_info(f'Статус сообщения {one}')
        log_info(r.json())
        in_progress_list[src]['IMESSTATUS'] = r.json()['State']
        in_progress_list[src]['CMESSDESC'] = r.json()['StateDescription']

    statement = 'update gis_dep_sms set IMESSTATUS = :IMESSTATUS, CMESSDESC =:CMESSDESC where CMESSAGEID =:CMESSAGEID'
    try:
        log_info('Сохраняем новые статусы в БД')
        with conn.connection.cursor() as cursor:
            cursor.executemany(statement, in_progress_list, batcherrors=True)
            conn.connection.commit()
            for error in cursor.getbatcherrors():
                log_error('Ошибка ' + str(error.code) + ' ' + str(error.message))
                log_error(in_progress_list[error.offset])
    except Exception as error:
        log_error('Ошибка сохранения новых статусов' + str(error))


def save_log():
    if log_messages:
        log_info('Сохранение log_messages')
        execute_many(log_messages)
    else:
        log_error(f'log_messages пуст. В БД логи не сохранены')

    if error_list:
        log_info('Сохранение error_list')
        execute_many(error_list)
    else:
        log_warn(f'error_list пуст. В БД логи не сохранены')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    current_balance()
    send_sms()
    save_log()
    current_balance()
    send_email(['ivgaide@domain.ru'], 'СМС-оповещение вкладчиков', f'СМС-рассылка о сроках окончания вкладов выполнена. Логи: {log_folder}')

    check_remaining_sms()

    
    
    
