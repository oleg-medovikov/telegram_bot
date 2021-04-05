import telebot,os,sqlalchemy,datetime
import pandas as pd  
import smtplib
from email.message import EmailMessage

#telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}

bot = telebot.TeleBot(os.getenv('telegram_bot'))
server  = os.getenv('server')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}")
con = eng.connect()

def send(group,text):
    sql = f"SELECT  [id] FROM [robo].[bot_users] where [groups] in ('master','{group}')"
    df = pd.read_sql(sql,con)
    ids = df['id'].unique()
    for id in ids:
        bot.send_message(id, text)
    return 1

def send_file(group,file):
    sql = f"SELECT  [id] FROM [robo].[bot_users] where [groups] in ('master','{group}')"
    df = pd.read_sql(sql,con)
    ids = df['id'].unique()
    for id in ids:
        bot.send_document(id, open(file, 'rb'))
    return 1

def voda(message):
    bot.send_message(message.from_user.id, 'Вы ввели\n' + message.text.lower())
    sql = f"select [full_name],email,cabinet from robo.bot_users where id = '{message.from_user.id}'"
    df  = pd.read_sql(sql,con)

    for email in ['EskovaT@spbmiac.ru', 'YahyaevaM@spbmiac.ru']:
        msg = EmailMessage()
        msg['Subject'] = 'Вода'
        msg['From'] = df.at[0,'email']
        msg['To']   = email
        msg.set_content('Добрый день, я ' + df.at[0,'full_name'] + '\n из кабинета ' + str(df.at[0,'cabinet']) \
                + '\nзабрал бутылку с номером ' + message.text.lower()  + '\n на дату: ' + datetime.datetime.now().strftime("%d.%m.%Y")     )

        with smtplib.SMTP('MIACMAIL.miacspb.zdrav.spb.ru', 587) as smtp:
            smtp.send_message(msg)

    bot.send_message(message.from_user.id, 'Я послал письмо')




