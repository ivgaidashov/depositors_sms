import os
import sys
import logging
from datetime import datetime
from smtplib import SMTP
from email.mime.text import MIMEText
from email.utils import formatdate
from email.mime.multipart import MIMEMultipart
from conf import log_folder, email_server, tkpb_email, tkpb_email_password

log_file_folder = log_folder+f'\\{datetime.today().strftime("%Y.%m.%d")}'
isExist = os.path.exists(log_file_folder)
if not isExist:
    os.makedirs(log_file_folder)

logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.StreamHandler(sys.stdout),
                        logging.FileHandler(log_file_folder+f'\sms_{datetime.today().strftime("%H-%M-%S")}.log', mode="a"),
                    ],
                    format='%(asctime)s: %(levelname)s - %(message)s')

def timestamp_to_date(date):
    if (date > -14182940000):
        new_date = datetime.fromtimestamp(date / 1000)
    else:
        new_date=None
    return new_date


def log_info(message):
    logging.info(message)

def log_warn(message):
    logging.warning(message)

def log_error(message):
    logging.error(message)

def send_email(recipients, subject, content):
    msg = MIMEMultipart()
    msg['from'] = 'hi@domain.ru'
    msg['to'] = ", ".join(recipients)
    msg['Date'] = formatdate()
    msg['subject'] = subject
    text = MIMEText(content, 'plain')
    msg.attach(text)
    log_info("Отправка уведомления по почте")
    try:
        server = SMTP()
        server.connect(email_server)
        server.login(tkpb_email, tkpb_email_password)
        server.sendmail(msg['from'], recipients, msg.as_string())
        log_info(f"Уведомление отправлено на {recipients}")
    except Exception as e:
        log_error(f"При отправке письма произошла ошибка: {e}")