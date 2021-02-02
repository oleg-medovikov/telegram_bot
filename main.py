import telebot,schedule,time,threading,os
# ========  мои модули 
from procedure import check_robot,svod_40_COVID_19
from reports import fr_deti,fr_status
from loader import search_file,check_file,excel_to_csv,load_fr,load_fr_death,load_fr_lab,slojit_fr,load_UMSRS,get_dir
from loader import medical_personal_sick
from sending import send_all,send_me
from presentation import generate_pptx

# ==========  настройки бота ============
#  используются переменные среды Windows
telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]

commands = """ \n
1) что в директории Robot?
2) отчет по детям
3) статус фр
4) сложить фр в один файл
5) загрузить фр
6) загрузить умерших
7) загрузить лабораторию
8) загрузить УМСРС
9) Взять директорию
10) Заболевший мед персонал
11) Свод по 40 COVID 19
12) Генерация презентации
"""
#===================================================
#============== Тут будут поток для расписаний =====


def load_1():
    def cool():
        if load_fr():
            if load_fr_death():
                if load_fr_lab():
                    return 1
    fr = threading.Thread(target=cool,name='Федеральный регистр')
    fr.start()

def load_2():
    umsrs = threading.Thread(target=load_UMSRS,name='УМСРС')
    umsrs.start()

def otchet_1():
    med_sick = threading.Thread(target=medical_personal_sick,name='Заболевший мед персонал')
    med_sick.start()


def go():
    while True:
        schedule.run_pending()
        time.sleep(1)

schedule.every().day.at("03:00").do(load_1)
schedule.every().day.at("06:00").do(load_2)
schedule.every().day.at("07:00").do(otchet_1)

t = threading.Thread(target=go, name="Расписание работ")
t.start()

#===================================================
def tread(func):
    threading.Thread(target=func).start 


@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if message.from_user.id in users_id:
        if message.text.lower() == 'привет':
            bot.send_message(message.from_user.id, 'Привет! Доступные команды:' + commands )
        if message.text.lower() in [ 'что в роботе' , '1']:
            bot.send_message(message.from_user.id, check_robot() )
        if message.text.lower() in [ 'отчет по детям' , '2']:
            bot.send_message(message.from_user.id, 'Минутку, сейчас посчитаю...')
            bot.send_document(message.from_user.id, open(fr_deti(), 'rb'))
        if message.text.lower() in ['статус фр','3']:
            bot.send_message(message.from_user.id, 'Хорошо, сейчас проверю...')
            bot.send_document(message.from_user.id, open(fr_status(), 'rb'))
        if message.text.lower() in ['сложить фр в один файл','4']:
            bot.send_message(message.from_user.id, 'Вот ничего без меня не можете...')
            bot.send_message(message.from_user.id, slojit_fr())
        if message.text.lower() in ['загрузить фр','5']:
            load_fr()
        if message.text.lower() in ['загрузить умерших','6']:
            load_fr_death()
        if message.text.lower() in ['загрузить лабораторию','7']:
            load_fr_lab()
        if message.text.lower() in ['загрузить УМСРС','8']:
            load_UMSRS()
        if message.text.lower() in ['9']:
            bot.send_message(message.from_user.id, get_dir('covid'))
        if message.text.lower() in ['10']:
            bot.send_message(message.from_user.id, tread(medical_personal_sick()))
        if message.text.lower() in ['11']:
            bot.send_message(message.from_user.id, 'Минутку, сейчас соберу...')
            svod_file = svod_40_COVID_19()
            if svod_file is not None:
                bot.send_document(message.from_user.id, open(svod_file, 'rb'))
                os.remove(svod_file)
            else:
                bot.send_message(message.from_user.id, ' Что я буду сводить?! Папка пустая!')
        if message.text.lower() in ['12']:
            bot.send_message(message.from_user.id, 'Да это же просто! Щас...')
            file_pptx = generate_pptx('2021-01-29')
            bot.send_document(message.from_user.id, open(file_pptx, 'rb'))
            os.remove(file_pptx)

bot.polling(none_stop=True)
