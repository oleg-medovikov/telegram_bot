import telebot,schedule,time,threading,os
# ======== мои модули 
from procedure import check_robot,svod_40_COVID_19,sort_death_mg
from reports import fr_deti,short_report,dead_not_mss,dynamics
from loader import search_file,check_file,excel_to_csv,load_fr,load_fr_death,load_fr_lab,slojit_fr,load_UMSRS,get_dir
from loader import medical_personal_sick,load_report_vp_and_cv
from sending import send_all,send_me
from presentation import generate_pptx
from zamechania_mz import no_snils,bez_izhoda,bez_ambulat_level,no_OMS,neveren_vid_lechenia,no_lab,net_diagnoz_covid,net_pad
import concurrent.futures
# ========== настройки бота ============

# используются переменные среды Windows
telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]

commands = """
1) что в директории Robot?
2) отчет по детям
3) статус фр
4) сложить фр в один файл
5) загрузить фр
6) загрузить умерших
7) загрузить лабораторию
8) загрузить УМСРС
9) Мониторинг ВП и ковид
10) Заболевший мед персонал
11) Свод по 40 COVID 19
12) Генерация презентации
13) Замечания МинЗдрава
14) Сортировка умерших по возрастам
15) Список умерших граждан в ФР без МСС
16) Отчет по динамике работы с ФР (в части приведения к данным УМСРС)
17) Справка губернатору
"""
commands_min="""
1. Нет СНИЛСа
2. Без исхода 45 дней
3. Нет амбулаторного этапа
4. Нет данных ОМС
5. неверный вид лечения
6. Нет лабораторного подтверждения
7. Нет диагноза COVID
8. Нет ПАД
"""
# =============== Процедурка создания потоков ====
def create_tred(func,arg):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(func,arg)
        try:
            return_value = future.result()
            return True, return_value
        except:
            return False, 'Ошибка при выполнении функции: ' +  str(future.exception())

#===================================================

#============== Тут будут поток для расписаний =====


def load_1():
    result = create_tred(load_fr,None)
    if result[0]:
        result = create_tred(load_fr_death,None)
        if result[0]:
            result = create_tred(load_fr_lab,None)
            if not result[0]:
                send_me(result[1])
        else:
            send_me(result[1])
    else:
        send_me(result[1])


def load_2():
    result = create_tred(load_UMSRS,None)
    if not result[0]:
        send_me(result[1])

def otchet_1():
    result = create_tred(medical_personal_sick,None)
    if not result[0]:
        send_me(result[1])


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

@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if message.from_user.id in users_id:
#========= приветсвие =====================
        if message.text.lower() == 'привет':
            bot.send_message(message.from_user.id, 'Привет! Доступные команды:' + commands )
#========== Простые репорты ===============
        if message.text.lower() in [ 'что в роботе' , '1']:
            bot.send_message(message.from_user.id, check_robot() )
        if message.text.lower() in [ 'отчет по детям' , '2']:
            bot.send_message(message.from_user.id, 'Минутку, сейчас посчитаю...')
            bot.send_document(message.from_user.id, open(fr_deti(), 'rb'))
        if message.text.lower() in ['статус фр','3']:
            bot.send_message(message.from_user.id, 'Хорошо, сейчас проверю...')
            result = create_tred(short_report,'Select * from robo.fr_status')
            if result[0]:
                bot.send_document(message.from_user.id, open(result[1], 'rb'))
            else:
                bot.send_message(message.from_user.id, result[1])
# ========= Загрузка в базу данных =======
        if message.text.lower() in ['сложить фр в один файл','4']:
            bot.send_message(message.from_user.id, 'Вот ничего без меня не можете...')
            bot.send_message(message.from_user.id, slojit_fr())
        if message.text.lower() in ['загрузить фр','5']:
            result = create_tred(load_fr,None)
            if not result[0]:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['загрузить умерших','6']:
            result = create_tred(load_fr_death,None)
            if not result[0]:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['загрузить лабораторию','7']:
            result = create_tred(load_fr_lab,None)
            if not result[0]:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['загрузить УМСРС','8']:
            result = create_tred(load_UMSRS,None)
            if not result[0]:
                bot.send_message(message.from_user.id, result[1])
# ========= Мониторинг Внебольничной пневмонии и ковида
        if message.text.lower() in ['9','Мониторинг ВП']:
            result = create_tred(load_report_vp_and_cv,None)
            if result[0]:
                if result[1][-3:] == 'png':
                    bot.send_message(message.from_user.id, 'Не найдены файлы должников')
                    bot.send_document(message.from_user.id, open(result[1], 'rb'))
                else:
                    bot.send_message(message.from_user.id, 'Создан файл' + '\n' + result[1].split('\\')[-1])
            else:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['10', 'Заболевший мед персонал']:
            result = create_tred(medical_personal_sick,None)
            if result[0]:
                pass
            else:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['11','Свод по 40 COVID 19']:
            bot.send_message(message.from_user.id, 'Минутку, сейчас соберу...')
            result = create_tred(svod_40_COVID_19,None)
            if result[0]:
                bot.send_document(message.from_user.id, open(result[1], 'rb'))
                os.remove(result[1])
            else:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['12','Генерация презентации']:
            bot.send_message(message.from_user.id, 'Да это же просто! Щас...')
            result = create_tred(generate_pptx,'2021-01-29')
            if result[0]:
                bot.send_document(message.from_user.id, open(result[1], 'rb'))
                os.remove(result[1])
            else:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['13','замечания минздрава']:
            bot.send_message(message.from_user.id, 'Что именно разложим?')
            bot.send_message(message.from_user.id, commands_min)
        if message.text.lower() in ['14','Сортировка умерших по возрастам']:
            bot.send_message(message.from_user.id, 'Давайте попробуем...')
            result = create_tred(sort_death_mg,None)
            if result[0]:
                bot.send_message(message.from_user.id, result[1])
            else:
                bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['15']:
            bot.send_message(message.from_user.id, 'Начинаю формировать список по ФР умерших без МСС')
            result = create_tred(dead_not_mss,None)
            bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['16']:
            bot.send_message(message.from_user.id, 'Начинаю формировать данные по динамике')
            result = create_tred(dynamics,None)
            bot.send_message(message.from_user.id, result[1])
        if message.text.lower() in ['17']:
            bot.send_message(message.from_user.id, 'Начинаю собирать данные')
            result = create_tred(dynamics,None)
            bot.send_message(message.from_user.id, result[1])


        # ============== Замечания минздрава =====================
        if message.text.lower() in ['1.']:
            if no_snils():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению Нет СНИЛСа.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['2.']:
            if bez_izhoda():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению Без исхода 45 дней.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['3.']:
            if bez_ambulat_level():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению Нет амбулаторного этапа.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['4.']:
            if bez_ambulat_level():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению Нет данных ОМС.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['5.']:
            if neveren_vid_lechenia():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению неверный вид лечения.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['6.']:
            if no_lab():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению нет лабораторного подтверждения.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['7.']:
            if net_diagnoz_covid():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению нет диагноза COVID.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)
        if message.text.lower() in ['8.']:
            if net_pad():
                bot.send_message(message.from_user.id, 'Уже разложил')
                file_stat = get_dir('temp') + '\\' + 'отчет по разложению нет ПАЗ.xlsx'
                bot.send_document(message.from_user.id, open(file_stat, 'rb'))
                os.remove(file_stat)




bot.polling(none_stop=True)
