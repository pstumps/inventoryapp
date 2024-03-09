import smtplib
from email.message import EmailMessage
from datetime import datetime
import schedule
import time
from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

db_conn = psycopg2.connect(host=os.getenv('DB_HOST'),dbname=os.getenv('DB_NAME'),user=os.getenv('DB_USER'),password=os.getenv('DB_PASSWORD'),port=os.getenv('DB_PORT'))

def getlowinventoryitems():
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM low_quantity_threshold")
    msg_content = []
    for row in cur:
        tablename = row[2]
        column_name = row[3]
        product = row[4]
        qty_threshold = row[5]
        cur_threshold = row[6]
        if cur_threshold <= qty_threshold:
            msg = F"{product} has {cur_threshold} left in {tablename}"
            msg_content.append(msg)

    return '\n'.join(msg_content)

#def sendnotifications(to, subject, body):
def sendnotifications():

    sendlist = []

    body = getlowinventoryitems()
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM low_quantity_alerts")
    for row in cur:
        phone_number = row[1]
        email_address = row[2]

        if phone_number:
            sendlist.append(phone_number)
        if email_address:
            sendlist.append(email_address)

        for count, contacts in enumerate(sendlist):

            msg = EmailMessage()
            msg.set_content(body)

            if count == 0:
                msg['to'] = contacts+'@vtext.com'
                msg['subject'] = ''
            if count == 1:
                msg['to'] = contacts
                msg['subject'] = 'Low Inventory Alert!'

            user = 'kungfutea.riverside@gmail.com'
            msg['from'] = user
            password = 'dokfmerloiaubafx'

            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(user, password)
            server.send_message(msg)

            server.quit()

def schedulealerts():
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM low_quantity_alerts")
    for row in cur:
        day_of_the_week = row[3]
        day_of_the_week = day_of_the_week.split(',')
        settime = row[4]
        settime = settime.strftime("%H:%M")
        #schedule.every().day.at('22:35').do(sendnotifications)
        schedule.every().day.at(settime).do(sendnotifications)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedulealerts()
    #sendnotifications()
    db_conn.close()