import telebot,os,pyodbc,datetime,smtplib
from email.message import EmailMessage



telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]


#admin = get_group_user_id('admin')
#epid = get_group_user_id('epid')
#support = get_group_user_id('support')


def send_admin(text):
    for id in admin:
        bot.send_message(id, text)

def send_epid(text):
    for id in epid:
        bot.send_message(id, text)

def send_support(text):
    for id in support:
        bot.send_message(id, text)

def send_all(text):
    for id in users_id:
        bot.send_message(id, text)

def send_me(text):
    bot.send_message(users_id[0], text)

def send_mail_with_excel(recipient_email, subject, content, excel_file):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = 'MedovikovOE@spbmiac.ru'
    msg['To'] = recipient_email
    msg.set_content(content)

    if excel_file is not None:
        with open(excel_file, 'rb') as f:
            file_data = f.read()
        msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=excel_file.split('\\')[-1])

    with smtplib.SMTP('MIACMAIL.miacspb.zdrav.spb.ru', 587) as smtp:
        smtp.send_message(msg)

def pismo_po_vode(emails):
    for email in emails.split(';'):
        send_mail_with_excel(email, 'Вода', 'Я взял воду для 604 кабинета ' + str(datetime.datetime.now()), None)
    return 'Я отправил письма'
