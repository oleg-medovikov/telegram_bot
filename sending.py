import telebot,os,pyodbc
#from main import user 

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
