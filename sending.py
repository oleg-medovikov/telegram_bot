import telebot,os,pyodbc

telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]

def send_all(text):
    for id in users_id:
        bot.send_message(id, text)

def send_me(text):
    bot.send_message(users_id[0], text)
