import telebot,schedule,time,threading,os
# ========  мои модули 
from check_robot import check_robot
from reports import fr_deti,fr_status
from loader import search_file,check_file,excel_to_csv,load_fr,load_fr_death,load_fr_lab
from sending import send_all,send_me

# ==========  настройки бота ============
#  используются переменные среды Windows
telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]

commands = """ \n
1) что в директории Robot?
2) отчет по детям
3) статус фр
4) конвертировать фр в csv
5) загрузить фр
6) загрузить умерших
7) загрузить лабораторию
"""
#===================================================
#============== Тут будут поток для расписаний =====

schedule.every().day.at("03:00").do(load_fr)
schedule.every().day.at("03:35").do(load_fr_death)
schedule.every().day.at("04:35").do(load_fr_lab)

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
        if message.text.lower() in ['конвертировать фр','4']:
            bot.send_message(message.from_user.id, 'Пробую конвертнуть')
            if search_file('fr')[0]:
                bot.send_message(message.from_user.id, 'Готовый файл: \n' + excel_to_csv(search_file('fr')[2]))
            else:
                bot.send_message(message.from_user.id, 'Я не нашёл файл фр!')
        if message.text.lower() in ['загрузить фр','5']:
            load_fr()
        if message.text.lower() in ['загрузить умерших','6']:
            load_fr_death()
        if message.text.lower() in ['загрузить лабораторию','7']:
            load_fr_lab()
bot.polling(none_stop=True)
