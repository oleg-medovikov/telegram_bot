#Работа с базами данных
import pandas as pd
import glob,warnings,datetime,os,xlrd,csv,openpyxl,shutil,threading
from sending import send_me,send_all
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from openpyxl.utils.dataframe import dataframe_to_rows
import win32com.client


warnings.filterwarnings('ignore')

con = create_engine(os.getenv('sql_engine'),convert_unicode=False)

def search_file(category):
    path = os.getenv('path_robot') + '\\' + datetime.datetime.now().strftime("%Y_%m_%d")
    if category == 'fr':
        pattern = path + r'\Федеральный регистр лиц, больных *[!ИАЦ].xlsx'
    if category == 'fr_death':
        pattern = path + r'\*Умершие пациенты*.xlsx'
    if category == 'fr_lab':
        pattern = path + r'\*Отчёт по лабораторным*.xlsx'
    file_excel = glob.glob(pattern)
    if len(file_excel):
        file_csv = glob.glob(pattern[:-4] + 'csv')
        if len(file_csv):
            return True, True, file_csv[0]
        else:
            return True, False, file_excel[0]
    else:
         return False, False, None

def check_file(file,category):
    if category == 'fr':
        names = [
'п/н','Дата создания РЗ','УНРЗ','Дата изменения РЗ','СНИЛС','ФИО','Пол','Дата рождения','Диагноз','Диагноз установлен','Осложнение основного диагноза','Субъект РФ','Медицинская организация','Ведомственная принадлежность','Вид лечения','Дата исхода заболевания','Исход заболевания','Степень тяжести','Посмертный диагноз','ИВЛ','ОРИТ','МО прикрепления','Медицинский работник'
                ]
    if category == 'fr_death':
        names = [
    '1', '2', '3', '4', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22','23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42','43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53'
                ]
    if category == 'fr_lab':
        names = [
'Субъект', 'УНРЗ', 'Мед. организация', 'Основной диагноз', 'Наименование лаборатории', 'Дата лабораторного теста','Тип лабораторного теста', 'Результат теста (положительный/ отрицательный)', 'Этиология пневмония','Дата первого лабораторного подтверждения COVID-19','Дата последнего лабораторного подтверждения COVID-19'
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


def slojit_fr():
    pathFolderFedRegParts = os.getenv('path_robot') +r'\_ФР_по_частям'
    date = datetime.datetime.today().strftime("%Y_%m_%d")
    nameSheetShablon = "Sheet1"
    _list = []
    list_numbers = ['раз', 'двас', 'трис']
    i = 0
    for excel in glob.glob(pathFolderFedRegParts + r'\Федеральный регистр*.xlsx'):
        df = pd.read_excel(excel, sheet_name= nameSheetShablon, dtype = str, skiprows = 1, head= 1)
        _list.append(df)
        try:
            send_all('прочтен файлик номер '+ list_numbers[i])
            i+=1
        except:
            pass
    svod = pd.DataFrame() 
    svod = pd.concat(_list)

    svod = svod[svod["Дата создания РЗ"].notnull()] 
    svod["п/н"] = range(1, len(svod)+1)
    
    new_fedreg = pathFolderFedRegParts + r'\Федеральный регистр лиц, больных COVID-19 - ' + date + '.xlsx'
    new_iach = pathFolderFedRegParts + r'\Федеральный регистр лиц, больных COVID-19 - ' + date + '_ИАЦ.xlsx'
#    shutil.copyfile(pathFolderFedRegParts + r'\ФР.xlsx', new_fedreg)
#    shutil.copyfile(pathFolderFedRegParts + r'\ФР_ИАЦ.xlsx', new_iach)
#    wb= openpyxl.load_workbook(new_fedreg)
#    ws = wb[nameSheetShablon]
#    rows = dataframe_to_rows(svod, index=False, header=False)
#    for r_idx, row in enumerate(rows, 1):
#        for c_idx, value in enumerate(row, 1):
#            ws.cell(row=r_idx, column=c_idx, value=value)
#    wb.save(new_fedreg)
#    svod = svod.drop(['СНИЛС', 'ФИО'], axis=1)
#    wb= openpyxl.load_workbook(new_iach)
#    ws = wb[nameSheetShablon]
#    rows = dataframe_to_rows(svod, index=False, header=False)
    def file_1(svod):
        with pd.ExcelWriter(new_fedreg) as writer:
            svod.to_excel(writer,index=False)

    def file_2(svod):
        df = svod
        del df['СНИЛС']
        del df['ФИО']
        with pd.ExcelWriter(new_iach) as writer:
            df.to_excel(writer,index=False)

    t_d1 = threading.Thread(target=file_1(svod),name='one')
    t_d2 = threading.Thread(target=file_2(svod),name='two')

    t_d1.start()
    t_d2.start()

#    for r_idx, row in enumerate(rows, 3):
#        for c_idx, value in enumerate(row, 1):
#            ws.cell(row=r_idx, column=c_idx, value=value)
#    wb.save(new_iach)

    NumberForMG = len(svod[svod['Диагноз'].isin(['U07.1']) \
                    & svod['Исход заболевания'].isnull() \
                    & svod['Вид лечения'].isin(['Стационарное лечение'])])

    NumberFor1 = len(svod[svod['Диагноз'].isin(['U07.1']) ])

    NumberFor2 = len(svod[svod['Посмертный диагноз'].isin(['U07.1']) \
                        & svod['Исход заболевания'].isin(['Смерть'])])

    otvet = 'Вроде все получилось \n' + 'По цифрам\n' \
            + 'На стационарном лечении: ' + str(NumberForMG) + '\n' \
            + 'Всего заболело: ' + str(NumberFor1) +'\n' \
            + 'Всего умерло: '+ str(NumberFor2)
    return otvet


def excel_to_csv_old(file_excel):
    file_csv = file_excel[:-4] + 'csv'
    sheet = xlrd.open_workbook(file_excel).sheet_by_index(0) 
    col = csv.writer(open(file_csv, 'w', newline=""),delimiter=";") 
    
    for row in range(sheet.nrows): 
        col.writerow(sheet.row_values(row))
    return file_csv

def excel_to_csv(file_excel):
    file_csv = file_excel[:-4] + 'csv'
    excel = openpyxl.load_workbook(file_excel)
    sheet = excel.active 
    col = csv.writer(open(file_csv, 'w', newline=""),delimiter=';')
    for r in sheet.rows: 
        col.writerow([cell.value for cell in r]) 
    return file_csv



def sql_execute(sql):
    Session = sessionmaker(bind=con)
    session = Session()
    session.execute(sql)
    session.commit()
    session.close()

def load_fr():
    def fr_to_sql(df):
        df  = df[df['Дата создания РЗ'] != '']
        del df ['Ведомственная принадлежность']
        del df ['Осложнение основного диагноза']
        send_all('Файл в памяти, количество строк: ' + str(len(df)) )
        sql_execute('TRUNCATE TABLE [dbo].[cv_input_fr]')
        send_all('Очистил input_fr')
        df.to_sql('cv_input_fr',con,schema='dbo',if_exists='append',index=False)
        send_all('Загрузил input_fr, запускаю процедуру')
        sql_execute('EXEC [dbo].[cv_Load_FedReg]')
        send_all('Федеральный регистр успешно загружен')
        return 1
    send_all('Я проснулся и хочу грузить фр!')
    search = search_file('fr')
    if search[0] and search[1]:
        send_all('Вот нашёлся такой файлик:\n' + search[2].split('\\')[-1])
        send_all('Сейчас я его проверю...')
        check = check_file(search[2],'fr')
        if check[0]:
            send_all('Файл прошёл проверку, начинаю грузить в память')
            df = pd.read_csv(search[2],header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
            fr_to_sql(df)
        else:
            send_all('Файл не прошёл проверку!')
            send_all(check[1])
            return 0
    else:
        if search[0]:
            send_all('Нее... я не хочу работать с xlsx, щас конвертирую!')
            file_csv = excel_to_csv(search[2]) 
            send_all('Результат:\n' + file_csv.split('\\')[-1])
            check = check_file(file_csv,'fr')
            if check[0]:
                send_all('Файл прошёл проверку, начинаю грузить в память')
                df = pd.read_csv(file_csv,header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
                fr_to_sql(df)
            else:
                send_all('Файл не прошёл проверку!')
                send_all(check[1])
                return 0
        else:
            send_all('Но я не нашёл файла федерального регистра (((')
            return 0
    return 1

def load_fr_death():
    def fr_death_to_sql(df):
        send_all('Обрезаю слишком длинные строки')
        for column in df.columns:
            for i in range(len(df)):
                df.loc[i,column]= str(df.at[i,column])[:255]
        send_all('Убираю Nan из таблицы')
        for column in df.columns:
            df[column] = df[column].str.replace(r'nan','')
        send_all('Отправляю в базу')
        df.to_sql('cv_input_fr_d_all_2',con,schema='dbo',if_exists='replace',index = False)
        send_all('Запускаю процедуры')
        sql_execute("""
                EXEC   [dbo].[Insert_Table_cv_input_fr_d_all_2]
                EXEC   [dbo].[cv_from_d_all_to_d_covid]
                EXEC   [dbo].[cv_Load_FedReg_d_All]
                EXEC   [dbo].[cv_Load_FedReg_d_covid]
                    """)
        send_all('Успешно загружено')
        return 1
    send_all('Пришло время спокойных')
    search = search_file('fr_death')
    if search[0] and search[1]:
        send_all('Найден файл:\n' + search[2].split('\\')[-1])
        send_all('Сейчас мы его рассмотрим...')
        check = check_file(search[2],'fr_death')
        if check[0]:
            send_all('Похоже файл в порядке, попробую загрузить')
            df = pd.read_csv(search[2],header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
            fr_death_to_sql(df)
        else:
            send_all('Что-то с файлом')
            send_all(str(check))
            return 0
    else:
        if search[0]:
            send_all('Давайте всё-таки работать с csv, конвертирую')
            file_csv = excel_to_csv(search[2]) 
            send_all('Результат:\n' + file_csv.split('\\')[-1])
            check = check_file(file_csv,'fr_death')
            if check[0]:
                send_all('Теперь можно и загрузить в память')
                df = pd.read_csv(file_csv,header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
                fr_death_to_sql(df)
            else:
                send_all('Файл не прошёл проверку!')
                send_all(check[1])
                return 0
        else:
            send_all('Не найден файл умерших')
        return 0

def load_fr_lab():
    def fr_lab_to_sql(df):
        send_all('Ну, тут надо переименовать колонки и можно грузить')
        i = 1
        for column in df.columns:
            df.rename(columns = {column: str(i)}, inplace = True)
            i+=1
        df = df.dropna(subset=['1','2','3','4','5','6'])
        send_all('Всего строк в  лабе '+ str(len(df)))
        df.to_sql('cv_input_fr_lab_2',con,schema='dbo',if_exists='replace',index = False)
        send_all('Остались процедуры в базе')
        sql_execute("""
                    EXEC   [dbo].[Insert_Table_cv_input_fr_lab_2]
                    EXEC   [dbo].[cv_load_frlab]
                    """)
        send_all('Лаборатория успешно загружена')
        return 1
    send_all('Посмотрим на файлик лабораторных исследований')
    search = search_file('fr_lab')
    if search[0] and search[1]:
        send_all('Найден файл:\n' + search[2].split('\\')[-1])
        send_all('Сейчас мы его рассмотрим...')
        check = check_file(search[2],'fr_lab')
        if check[0]:
            send_all('Неужели файл все-таки можно загрузить?')
            df = pd.read_csv(search[2],header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
            fr_lab_to_sql(df)
        else:
            send_all('Не, плохой файл!')
            send_all(str(check))
            return 0
    else:
        if search[0]:
            send_all('Ну, это точно нужно перевести в csv!')
            file_csv = excel_to_csv(search[2]) 
            send_all('Результат:\n' + file_csv.split('\\')[-1])
            check = check_file(file_csv,'fr_lab')
            if check[0]:
                send_all('Теперь можно и загрузить в память')
                df = pd.read_csv(file_csv,header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
                fr_lab_to_sql(df)
            else:
                send_all('Файл не прошёл проверку!')
                send_all(check[1])
                return 0
        else:
            send_all('Не найден файл лаборатории')
        return 0

