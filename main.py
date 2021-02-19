import telebot,schedule,time,threading,os,random,pyodbc
from dataclasses import dataclass
import concurrent.futures

# ======== мои модули 
from procedure import check_robot,svod_40_COVID_19,sort_death_mg,medical_personal_sick,razlojit_death_week,sbor_death_week_files,sbor_death_week_svod
from reports import fr_deti,short_report,dead_not_mss,dynamics,mg_from_guber
from loader import search_file,check_file,excel_to_csv,load_fr,load_fr_death,load_fr_lab,slojit_fr,load_UMSRS,get_dir
from loader import load_report_vp_and_cv
from sending import send_all,send_me
from presentation import generate_pptx
from zamechania_mz import no_snils,bez_izhoda,bez_ambulat_level,no_OMS,neveren_vid_lechenia,no_lab,net_diagnoz_covid,net_pad,net_dnevnik
from regiz import regiz_decomposition
from send_ODLI import send_bundle_to_ODLI
# ========== настройки бота ============

# используются переменные среды Windows
telebot.apihelper.proxy = {'https': os.getenv('http_proxy')}
bot = telebot.TeleBot(os.getenv('telegram_bot'))
users_id=[int(x) for x in os.getenv('telegram_id').split(',')]
conn = pyodbc.connect(os.getenv('sql_conn'))
cursor= conn.cursor()
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

def load_1():
    result = create_tred('load_fr',None)
    if result[0]:
        result = create_tred('load_fr_death',None)
        if result[0]:
            result = create_tred('load_fr_lab',None)
            if not result[0]:
                send_me(result[1])
        else:
            send_me(result[1])
    else:
        send_me(result[1])

def load_2():
    result = create_tred('load_UMSRS',None)
    if not result[0]:
        send_me(result[1])

def otchet_1():
    result = create_tred('medical_personal_sick',None)
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

#================Создаем классы для команд и пользователей
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

    def get_commands():
        commands.clear()
        for row in cursor.execute('exec robo.bot_command_table'):
            commands.append(command(int(row[0])
                                    ,row[1]
                                    ,[str(x) for x in row[2].split(',')]
                                    ,row[3]
                                    ,row[4]
                                    ,row[5]
                                    ,[str(x) for x in row[6].split(';')]
                                    ,[int(x) for x in row[7].split(',')]
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
    def number(id,mess):
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
        for row in cursor.execute('select id,short_name,groups from robo.bot_users'):
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

# ========== Главная процедура бота ===============
@bot.message_handler(content_types=['text'])
def get_text_messages(message):
    if user.known(message.from_user.id):
        if message.text.lower() in ['привет', 'ghbdtn','/start','старт']:
            bot.send_message(message.from_user.id, get_hello_start() + user.name(message.from_user.id))
            bot.send_message(message.from_user.id,command.hello_mess(message.from_user.id))
        else:
            if command.number(message.from_user.id,message.text.lower())[0]:
                command_id = command.number(message.from_user.id,message.text.lower())[1]
                bot.send_message(message.from_user.id,random.choice(commands[command_id].hello_words))
                result = create_tred(commands[command_id].procedure_name,commands[command_id].procedure_arg)
                if result[0]:
                    if commands[command_id].return_file:
                        bot.send_document(message.from_user.id, open(result[1], 'rb'))
                        os.remove(result[1])
                        if message.from_user.id != user.master():
                            bot.send_message(user.master(), 'Хозяин, для пользователя ' + user.name(message.from_user.id) + ' было выполнено задание ' + str(command_id))
                    else:
                        bot.send_message(message.from_user.id, result[1])
                        if message.from_user.id != user.master():
                            bot.send_message(user.master(), 'Хозяин, для пользователя ' + user.name(message.from_user.id) + ' было выполнено задание ' + str(command_id))
                else:
                    bot.send_message(message.from_user.id, result[1])
                    if message.from_user.id != user.master():
                        bot.send_message(user.master(),\
                            'Хозяин, что-то сломалось при выполнении задания ' + str(command_id) + ' для пользователя ' + user.name(message.from_user.id) )
                        bot.send_message(user.master(), result[1])
                
            else:
                bot.send_message(message.from_user.id,'Возможно, у Вас нет прав на выполнение данной операции')
                if message.from_user.id != user.master():
                    bot.send_message(user.master(), 'Хозяин, пользователь ' + user.name(message.from_user.id) + ' прислал мне это:' )
                    bot.send_message(user.master(),message.text.lower())
    else:
        bot.send_message(message.from_user.id,'Я вас не знаю!')
        bot.send_message(user.master(),'Мне написал неизвесный пользователь!')
        bot.send_message(user.master(),str(message.from_user.id))

bot.polling(none_stop=True)
