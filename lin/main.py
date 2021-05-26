#!/usr/bin/env python
import telebot,schedule,time,threading,os,random,datetime,sqlalchemy
from dataclasses import dataclass
import concurrent.futures

# ======== мои модули 
from procedure import check_robot,svod_40_COVID_19,sort_death_mg,medical_personal_sick,razlojit_death_week,sbor_death_week_files,sbor_death_week_svod
from procedure import svod_unique_patient,svod_vachine_dates
from reports import fr_deti,short_report,dead_not_mss,dynamics,mg_from_guber
from loader import search_file,check_file,excel_to_csv,load_fr,load_fr_death,load_fr_lab,slojit_fr,load_UMSRS,get_dir
from loader import load_report_vp_and_cv,load_report_guber,load_vaccina
from sending import send,voda,send_file
from presentation import generate_pptx
from zamechania_mz import no_snils,bez_izhoda,bez_ambulat_level,no_OMS,neveren_vid_lechenia,no_lab,net_diagnoz_covid,net_pad,net_dnevnik,delete_old_files,load_snils_comment
from zamechania_mz import zavishie_statusy
from regiz import regiz_decomposition,regiz_load_to_base
from parus import o_40_covid_by_date,svod_40_cov_19,parus_43_cov_nulls,svod_43_covid_19,no_save_43,cvod_29_covid
from parus import cvod_33_covid,cvod_36_covid,cvod_37_covid,cvod_38_covid,cvod_26_covid
#from send_ODLI import send_bundle_to_ODLI
import telebot_calendar
from telebot_calendar import CallbackData
from telebot.types import ReplyKeyboardRemove,CallbackQuery
# ========== настройки бота ============

# используются переменные среды 
#telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))

server  = os.getenv('server')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser') # Тут правильный двойной слеш!
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()

#================Создаём классы для команд и пользователей
commands = []
@dataclass
class command: 
    command_id     : int
    command_name   : str
    command_key    : list
    procedure_name : str
    procedure_arg  : str
    return_file    : bool
    hello_words    : list
    ids            : list
    ask_day        : bool

    def get_commands():
        commands.clear()
        for row in con.execute('exec robo.bot_command_table'):
            commands.append(command(int(row[0])
                                    ,row[1]
                                    ,[str(x) for x in row[2].split(',')]
                                    ,row[3]
                                    ,row[4]
                                    ,row[5]
                                    ,[str(x) for x in row[6].split(';')]
                                    ,[int(x) for x in row[7].split(',')]
                                    ,row[8]
                                   ))
    def search(key):
        for command in commands:
            if key in command.command_key:
                return command
    def check_id(id,command):
        if id in command.ids:
            return True
        else:
            return False
    def hello_mess(id):
        string = 'Доступные Вам комманды:'
        for command in commands:
            if id in command.ids:
                string += '\n' + command.command_key[0] +') ' + command.command_name
        return string
    def number( id,mess):
        for command in commands:
            if id in command.ids and mess in command.command_key:
                return True, command.command_id
        return False, None

users = []
@dataclass
class user:
    user_id    : int
    short_name : str
    groups     : str
    
    def get_users():
        users.clear()
        for row in con.execute('select id,short_name,groups from robo.bot_users'):
            users.append(user(int(row[0]),row[1],row[2]) ) 

    def known(id):
        for user in users:
            if user.user_id == id:
                return True
        return False
    
    def name(id):
        for user in users:
            if user.user_id == id:
                return user.short_name
            
    def group(id):
        for user in users:
            if user.user_id == id:
                return user.groups
    def group_users_id(name_group):
        list_=[]
        for user in users:
            if user.groups == name_group:
                list_.append(user.user_id)
        return list_
    def master():
        for user in users:
            if user.groups == 'master':
                return user.user_id
command.get_commands()
user.get_users()

def get_group_user_id(group_name):
    list_ = []
    for user in users:
        if user.groups == group_name:
            list_.append(user_id)
    return list_

# =============== Процедурка создания потоков ====
def create_tred(func,arg):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(globals()[func],arg)
        try:
            return_value = future.result()
            return True, return_value
        except:
            return False, 'Ошибка при выполнении функции: ' +  str(future.exception())

#===================================================

#============== Тут будут поток для расписаний =====
def log_shedule(work,result):
    if not result[0]:
        send('admin', 'При выполнении ' + work + ' произошло\n' + result[1])
    else:
        send('admin', 'Выполнил ' + work)

    sql = f"""
    INSERT INTO [robo].[bot_logs]
	       ([user],[task],[schedule],[success],[result])
	 VALUES
	       ('devil'
	       ,'{work}'
	       ,'True'
	       ,'{result[0]}'
	       ,'{str(result[1]).replace("'","")}')
    """
    con.execute(sql)


def load_1():
    result = create_tred('load_fr',None)
    work = 'загрузка фр'
    log_shedule(work, result)
    if result[0]:
        result = create_tred('load_fr_death',None)
        work = 'загрузка умерших'
        log_shedule(work, result)
        if result[0]:
            result = create_tred('load_fr_lab',None)
            work = 'загрузка лаборатории'
            log_shedule(work, result)

def load_2():
    result = create_tred('load_UMSRS',None)
    log_shedule('загрузка УМСРС', result)

def otchet_1():
    result = create_tred('medical_personal_sick',None)
    work = 'файл заболевших медработников' 
    log_shedule(work, result)

def regiz_razlogenie():
    send('info', 'РЕГИЗ Начинаю раскладывать файлы по папкам')
    result = create_tred('regiz_decomposition', None )
    work = 'РЕГИЗ разложение файлов' 
    log_shedule(work, result)
    if not result[0]:
        send('info', result[1])
    else:
        try:
            os.remove(result[1])
        except:
            pass

def regiz_load():
    send('info', 'РЕГИЗ Начинаю загружать файлы')
    result = create_tred('regiz_load_to_base',None)
    work = 'РЕГИЗ загрузка файлов' 
    log_shedule(work, result)
    if not result[0]:
        send('info', result[1])

def go():
    while True:
        schedule.run_pending()
        time.sleep(1)

schedule.every().day.at("03:00").do(load_1)
schedule.every().day.at("06:00").do(load_2)
schedule.every().day.at("07:00").do(otchet_1)
schedule.every().day.at("07:05").do(regiz_razlogenie)
schedule.every().day.at("16:05").do(regiz_load)

t = threading.Thread(target=go, name="Расписание работ")
t.start()

# ========= Маленькая процедурка для определения периода суток

def get_hello_start():
    temp = int(time.strftime("%H"))
    return {
         0   <= temp   < 6  :  'Доброй ночи, ',
         6   <= temp   < 11 :  'Доброе утро, ',
         11  <= temp   < 16 :  'Добрый день, ',
         16  <= temp   < 22 :  'Добрый вечер, ',
         22  <= temp   < 24 :  'Доброй ночи, '
    }[True]

# ========= Процедура логирования =================
def logi(user_id,command_id,result):
    if result[0]:
        if user_id != user.master():
            bot.send_message(user.master(), 'Хозяин, для пользователя ' + user.name(user_id) + ' было выполнено задание \n' + commands[command_id].command_name)
    else:
        if user_id != user.master():
            bot.send_message(user.master(), 'Хозяин, у пользователя ' + user.name(user_id) \
                    + ' что-то пошло не так с заданием \n' + commands[command_id].command_name \
                    + '\n' + result[1])
    sql = f"""
    INSERT INTO [robo].[bot_logs]
	       ([user],[task],[schedule],[success],[result])
	 VALUES
	       ('{user.name(user_id)}'
	       ,'{commands[command_id].command_name}'
	       ,'False'
	       ,'{result[0]}'
	       ,'{str(result[1]).replace("'","")}')
    """
    con.execute(sql)

# ========== Главная процедура бота ===============
@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if user.known(message.from_user.id):
        if message.text.lower() in ['привет', 'ghbdtn','/start','старт']:
            bot.send_message(message.from_user.id, get_hello_start() + user.name(message.from_user.id))
            bot.send_message(message.from_user.id,command.hello_mess(message.from_user.id))
        elif message.text.lower() in ['Вода','djlf','вода']:
            mes = bot.reply_to(message, 'Здравствуйте '+ user.name(message.from_user.id) + ' напишите номер бутылочки.')
            bot.register_next_step_handler(mes, voda)
            
        else:
            if command.number(message.from_user.id,message.text.lower())[0]:
                command_id = command.number(message.from_user.id,message.text.lower())[1]
                bot.send_message(message.from_user.id,random.choice(commands[command_id].hello_words))
                if commands[command_id].ask_day:
                    calendar_1 = CallbackData("calendar_1", "action", "year", "month", "day")
                    bot.send_message(message.from_user.id,'с помощью клавиатуры',reply_markup=telebot_calendar.create_calendar(
                                    name=calendar_1.prefix,
                                    year=datetime.datetime.now().year,
                                    month=datetime.datetime.now().month ) )
                    @bot.callback_query_handler(func=lambda call: call.data.startswith(calendar_1.prefix))
                    def callback_inline(call: CallbackQuery):
                        name, action, year, month, day = call.data.split(calendar_1.sep)
                        date = telebot_calendar.calendar_query_handler(
                                        bot=bot, call=call, name=name, action=action, year=year, month=month, day=day
                                            )
                        if action == "DAY":
                            bot.send_message(chat_id=call.from_user.id,
                                            text=f"Вы выбрали {date.strftime('%d.%m.%Y')}",
                                            reply_markup=ReplyKeyboardRemove(),)
                            bot.send_message(call.from_user.id,commands[command_id].procedure_name)
                            result = create_tred(commands[command_id].procedure_name,date)
                            bot.send_message(call.from_user.id,result[1])
                            logi(call.from_user.id,command_id,result)
                        elif action == "CANCEL":
                            bot.send_message(chat_id=call.from_user.id,
                                    text="Вы решили ничего не выбирать",reply_markup=ReplyKeyboardRemove(),)
                else:
                    result = create_tred(commands[command_id].procedure_name,commands[command_id].procedure_arg)
                    if result[0]:
                        if commands[command_id].return_file:
                            for file in result[1].split(';'):
                                bot.send_document(message.from_user.id, open(file, 'rb'))
                                os.remove(file)
                                logi(message.from_user.id,command_id,result)
                        else:
                            bot.send_message(message.from_user.id, result[1])
                            logi(message.from_user.id,command_id,result)
                    else:
                        bot.send_message(message.from_user.id, result[1])
                        logi(message.from_user.id,command_id,result)
            else:
                bot.send_message(message.from_user.id,'Возможно, у Вас нет прав на выполнение данной операции')
                if message.from_user.id != user.master():
                    bot.send_message(user.master(), 'Хозяин, пользователь ' + user.name(message.from_user.id) + ' прислал мне это:' )
                    bot.send_message(user.master(),message.text.lower())
    else:
        bot.send_message(message.from_user.id,'Я вас не знаю!')
        bot.send_message(user.master(),'Мне написал неизвесный пользователь!')
        bot.send_message(user.master(),str(message.from_user.id))
        bot.send_message(user.master(),str(message.from_user.username))

bot.polling(none_stop=True)
