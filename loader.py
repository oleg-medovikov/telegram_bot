#Работа с базами данных
import pandas as pd
import glob,warnings,datetime,os,xlrd,csv
from sending import send_me
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker


warnings.filterwarnings('ignore')

con = create_engine(os.getenv('sql_engine'),convert_unicode=False)

def search_file(category):
    path = os.getenv('path_robot') + '\\' + datetime.datetime.now().strftime("%Y_%m_%d")
    if category == 'fr':
        file_excel = glob.glob(path + r'\Федеральный регистр лиц, больных *[!ИАЦ].xlsx')
    if len(file_excel):
        file_csv = glob.glob(path + r'\Федеральный регистр лиц, больных *[!ИАЦ].csv')
        if len(file_csv):
            return True, True, file_csv[0]
        else:
            return True, False, file_excel[0]
    else:
        return False, False, None

def check_file(file,category):
    if category == 'fr':
        names = [
                'п/н','Дата создания РЗ','УНРЗ','Дата изменения РЗ','СНИЛС','ФИО','Пол','Дата рождения','Диагноз',
                'Диагноз установлен','Осложнение основного диагноза','Субъект РФ','Медицинская организация','Ведомственная принадлежность',
                'Вид лечения','Дата исхода заболевания','Исход заболевания','Степень тяжести',
                'Посмертный диагноз','ИВЛ','ОРИТ','МО прикрепления','Медицинский работник'
                ]
    sum_colum = len(names)
    names_found = []
    num_colum = 0
    try:
        file =  pd.read_csv(file, delimiter=';', engine='python')
    except:
        return [False,'Файл не читается','',0]
    for head in range(len(file)):
        if num_colum != sum_colum:
            coll = file.loc[head].tolist()
            num_colum = 0
            collum = []
            check = True
            error_text = '' 
            for name in names:
                k = 0
                f = 0
                for col in coll:
                    if str(col).replace(' ','') == name.replace(' ',''): 
                        collum.append(k)
                        names_found.append(name)
                        num_colum += 1 
                        f = 1
                    k += 1
        else:
            break
    if len(names_found) < sum_colum:
        check = False
        error_text = ' Не найдена колонка ' + str(list(set(names) - set(names_found) ) ) + ';'
        return check,error_text,collum, head
    return check,error_text,collum, head

def excel_to_csv(file_excel):
    file_csv = file_excel[:-4] + 'csv'
    sheet = xlrd.open_workbook(file_excel).sheet_by_index(0) 
    col = csv.writer(open(file_csv, 'w', newline=""),delimiter=";") 
    
    for row in range(sheet.nrows): 
        col.writerow(sheet.row_values(row))
    return file_csv


def sql_execute(sql):
    Session = sessionmaker(bind=con)
    session = Session()
    session.execute(sql)
    session.commit()
    session.close()

def load_fr():
    def fr_to_sql(df):
        print(df.head(4))
        df  = df[df['Дата создания РЗ'] != '']
        del df ['Ведомственная принадлежность']
        del df ['Осложнение основного диагноза']
        send_me('Файл в памяти, количество строк: ' + str(len(df)) )
        sql_execute('TRUNCATE TABLE [dbo].[cv_input_fr]')
        send_me('Очистил input_fr')
        df.to_sql('cv_input_fr',con,schema='dbo',if_exists='append',index=False)
        send_me('Загрузил input_fr, запускаю процедуру')
        sql_execute('EXEC [dbo].[cv_Load_FedReg]')
        send_me('Федеральный регистр успешно загружен')
        return 1
    send_me('Я проснулся и хочу грузить фр!')
    search = search_file('fr')
    if search[0] and search[1]:
        send_me('Вот нашёлся такой файлик:\n' + search_file('fr')[2])
        send_me('Сейчас я его проверю...')
        check = check_file(search[2],'fr')
        if check[0]:
            send_me('Файл прошёл проверку, начинаю грузить в память')
            df = pd.read_csv(search[2],header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
            fr_to_sql(df)
        else:
            send_me('Файл не прошёл проверку!')
            send_me(check[1])
    else:
        if search[0]:
            send_me('Нее... я не хочу работать с xlsx, щас конвертирую!')
            file_csv = excel_to_csv(search[2]) 
            send_me('Результат:\n' + file_csv)
            check = check_file(file_csv,'fr')
            if check[0]:
                send_me('Файл прошёл проверку, начинаю грузить в память')
                df = pd.read_csv(file_csv,header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
                fr_to_sql(df)
            else:
                send_me('Файл не прошёл проверку!')
                send_me(check[1])
        else:
            send_me('Но я не нашёл файла федерального регистра (((')

