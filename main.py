import telebot,schedule,time,threading,os
# ========  мои модули 
from check_robot import check_robot
from reports import fr_deti,fr_status

# ==========  настройки бота ============
#  используются переменные среды Windows
telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]

commands = """ \n
1) что в директории Robot? \n
2) отчет по детям \n
3) статус фр \n
4) ...
"""
#===================================================
#============== Тут будут поток для расписаний =====
def job():
    for id in users_id:
        bot.send_message(id, 'Я работаю!')

schedule.every().day.at("10:30").do(job)

def go():
    while True:
        schedule.run_pending()
        time.sleep(1)

t = threading.Thread(target=go, name="тест")
t.start()



#===================================================
@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if message.from_user.id in users_id:
        if message.text.lower() == 'привет':
            bot.send_message(message.from_user.id, 'Привет! Доступные команды:' + commands )
        if message.text.lower() in [ 'что в роботе' , '1']:
            bot.send_message(message.from_user.id, check_robot())
        if message.text.lower() in [ 'отчет по детям' , '2']:
            bot.send_message(message.from_user.id, 'Минутку, сейчас посчитаю...')
            bot.send_document(message.from_user.id, open(fr_deti(), 'rb'))
        if message.text.lower() in ['статус фр','3']:
            bot.send_message(message.from_user.id, 'Хорошо, сейчас проверю...')
            bot.send_document(message.from_user.id, open(fr_status(), 'rb'))


bot.polling(none_stop=True)
