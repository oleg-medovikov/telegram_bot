import telebot,os,sqlalchemy,datetime,cx_Oracle
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

base_parus = os.getenv('base_parus')
userName = os.getenv('oracle_user')
password = os.getenv('oracle_pass')
userbase = os.getenv('oracle_base')


class my_except(Exception):
    pass

 
def send(group,text):
    sql = f"SELECT  [id] FROM [robo].[bot_users] where [groups] in ('master','{group}')"
    df = pd.read_sql(sql,con)
    ids = df['id'].unique()
    for id in ids:
        try:
            bot.send_message(id, text)
        except:
            pass
    return 1

def send_file(group,file):
    sql = f"SELECT  [id] FROM [robo].[bot_users] where [groups] in ('master','{group}')"
    df = pd.read_sql(sql,con)
    ids = df['id'].unique()
    for id in ids:
        try:
            bot.send_document(id, open(file, 'rb'))
        except:
            pass
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

def send_parus(mess):
    try:
        id_chat_parus = int(os.getenv('id_chat_parus'))
    except:
        raise my_except('Не найден id чата парус')
    else:
        mess = mess.replace('today', datetime.datetime.now().strftime('%d.%m.%Y'))
        mess = mess.replace('yesterday', (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d.%m.%Y'))

        mes = f"""\U0001F916 Добрый день, уважаемые коллеги! \u23F0 \n{mess}"""
        bot.send_message(id_chat_parus, mes, parse_mode= 'Markdown')
        return 1

def send_Debtors(argument):
    def spisok_mo(id_cov):
        if id_cov == 33:
            sql   = open('sql/dolg/covid_33.sql','r').read()
        elif id_cov == 36:
            sql   = open('sql/dolg/covid_36.sql','r').read()
        elif id_cov == 37:
            sql   = open('sql/dolg/covid_37.sql','r').read()
        elif id_cov == 38:
            sql   = open('sql/dolg/covid_38.sql','r').read()
        elif id_cov == 51:
            sql   = open('sql/dolg/covid_51.sql','r').read()
        elif id_cov == 155:
            sql   = open('sql/dolg/dist_cons.sql','r').read()
        else:
            return 1
        with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
            df = pd.read_sql(sql,con)
        
        return df
    try:
        id_cov = int(argument.split(';')[0])
    except:
        raise my_except('не понятный аргумент\n', argument)
    
    try:
        mess = argument.split(';')[1]
    except:
        raise my_except('не понятный аргумент\n', argument)
    
    try:
        id_chat_parus = int(os.getenv('id_chat_parus'))
    except:
        raise my_except('Не найден id чата парус')
    else:
        if id_cov == 155:
            monitoring = 'ДистанцКонсультации'
        else:
            monitoring = str(id_cov) + ' COVID 19'

        sql = f"SELECT Distinct [NameMOParus] FROM [COVID].[robo].[DebtorsReport] WHERE [Report] = '{monitoring}' AND [IsAction] = 1"
        base = pd.read_sql(sql,con)
        organization = spisok_mo(id_cov)

        Debtors = base.merge(organization,how='left',left_on='NameMOParus', right_on='ORGANIZATION')
        Debtors = Debtors.loc[Debtors['ORGANIZATION'].isnull()]

        mess = mess.replace('today', datetime.datetime.now().strftime('%d.%m.%Y'))
        mess = mess.replace('yesterday', (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d.%m.%Y') )

       
        mes = f"""\U0001F916 Добрый день, уважаемые коллеги! \u23F0 \n{mess}\nНа данный момент от *{len(Debtors)}* МО не получены отчёты:"""

        for mo in Debtors['NameMOParus']:
            if len(mes) > 4000:
                bot.send_message(id_chat_parus, mes, parse_mode= 'Markdown')
                mes = ''
            mes += '\n' + mo

        bot.send_message(id_chat_parus, mes, parse_mode= 'Markdown')
        return 1
