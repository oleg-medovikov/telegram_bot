#Работа с базами данных
import pandas as pd
import glob,warnings,datetime,os,xlrd,csv,openpyxl,shutil,threading,pyodbc
from sending import send_me,send_all
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from openpyxl.utils.dataframe import dataframe_to_rows
import win32com.client


warnings.filterwarnings('ignore')

con = create_engine(os.getenv('sql_engine'),convert_unicode=False)
conn = pyodbc.connect(os.getenv('sql_conn'))

def get_dir(name):
    sql = f"SELECT Directory FROM [robo].[directions_for_bot] where NameDir = '{name}'"
    df = pd.read_sql(sql,con)
    return df.iloc[0,0] 

def check_table(name):
    sql=f"""
    SELECT distinct case when (cast([datecreated] as date) =  cast(getdate() as date))
        then 1
        else 0
        end AS 'Check'
            FROM [dbo].[cv_{name}]
    """
    return pd.read_sql(sql,conn).iat[0,0]

def search_file(category):
    path = os.getenv('path_robot') + '\\' + datetime.datetime.now().strftime("%Y_%m_%d")
    if category == 'fr':
        pattern = path + r'\Федеральный регистр лиц, больных *[!ИАЦ].xlsx'
    if category == 'fr_death':
        pattern = path + r'\*Умершие пациенты*.xlsx'
    if category == 'fr_lab':
        pattern = path + r'\*Отчёт по лабораторным*.xlsx'
    if category == 'UMSRS':
        pattern = path + r'\*УМСРС*.xlsx'
    file_excel = glob.glob(pattern)
    file_csv = glob.glob(pattern[:-4] + 'csv')
    if len(file_csv):
        return True, True, file_csv[0]
    else:
        if len(file_excel):
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
    if category == 'UMSRS':
        names = [
    '№ п/п', 'Номер свидетельства о смерти', 'Дата выдачи', 'Категория МС', 'Фамилия', 'Имя', 'Отчество', 'Пол', 'Дата рождения','Дата смерти', 'Возраст', 'Страна', 'Республика', 'Субъект', 'Область', 'Район', 'Город', 'Населенный пункт', 'Элемент планировочной структуры','Район СПБ', 'Улица', 'Дом', 'Корпус', 'Строение', 'Квартира', 'Страна смерти', 'Республика смерти','Субъект смерти', 'Область смерти','Район смерти', 'Город смерти', 'Населенный пункт смерти', 'Элемент планировочной структуры смерти', 'Район СПБ смерти', 'Улица смерти','Дом смерти', 'Корпус смерти', 'Строение смерти', 'Квартира смерти', 'Место смерти', 'Код МКБ-10 а', 'Болезнь или состояние, непосред приведшее к смерти','Код МКБ-10 б', 'Патол. состояние, кот. привело к указанной причине', 'Код МКБ-10 в', 'Первоначальная причина смерти', 'Код МКБ-10 г','Внешняя причина при травмах и отравлениях','Код II', 'Прочие важние состояния', 'Код МКБ-10 а(д)', 'Основное заболевание плода или ребенка','Код МКБ-10 б(д)', 'Другие заболевания плода или ребенка', 'Код МКБ-10 в(д)', 'Основное заболевание матери', 'Код МКБ-10 г(д)','Другие заболевания матери', 'Код МКБ-10 д(д)', 'Другие обстоятельства мертворождения', 'Установил причины смерти', 'Адрес МО','Краткое наименование', 'Основания установления причин смерти', 'Осмотр трупа', 'Записи в мед.док.', 'Предшествующего наблюдения','Вскрытие', 'Статус МС', 'Взамен', 'Дубликат', 'Испорченное', 'Напечатано', 'в случае смерти результате ДТП'
                ]
    sum_colum = len(names)
    names_found = []
    num_colum = 0
    try:
        file =  pd.read_csv(file,header=None, delimiter=';', engine='python')
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
        return check,error_text,collum, head - 1
    return check,error_text,collum, head - 1

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
    try:
        del df['Unnamed: 24']
    except:
        pass
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
        if check_table('fedreg'):
            send_all('Федеральный регистр успешно загружен')
            return 1
        else:
            send_all('Произошла какая-то проблема с загрузкой фр')
            return 0
    send_all('Я проснулся и хочу грузить фр!')
    search = search_file('fr')
    if search[0] and search[1]:
        send_all('Вот нашёлся такой файлик:\n' + search[2].split('\\')[-1])
        send_all('Сейчас я его проверю...')
        check = check_file(search[2],'fr')
        if check[0]:
            send_all('Файл прошёл проверку, начинаю грузить в память')
            df = pd.read_csv(search[2],header = check[3], usecols = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
            print(df.head(3))
            fr_to_sql(df)
            return 1
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
                return 1
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
            return 1
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
                return 1
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
        if check_table('fedreg_lab'):
            send_all('Лаборатория успешно загружена')
            return 1
        else:
            send_all('Какая-то проблема с загрузкой лаборатории')
            return 0
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
            return 1
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
                return 1
            else:
                send_all('Файл не прошёл проверку!')
                send_all(check[1])
                return 0
        else:
            send_all('Не найден файл лаборатории')
        return 0

def load_UMSRS():
    def UMSRS_to_sql(df):
        df.to_sql('cv_input_umsrs_2',con,schema='dbo',if_exists='append',index = False)
        send_all('Данные загружены в input_umsrs_2, запускаю процедурки')
        sql_execute("""
                    EXEC   [dbo].[Insert_Table_cv_input_umsrs_2]
                    EXEC   [dbo].[cv_Load_UMSRS]
                    """)
        if check_table('umsrs'):
            send_all('Успешно выполнено!')
            return 1
        else:
            send_all('Какая-то проблема с загрузкой УМСРС')
            return 0
    send_all('А теперь будем грузить УМСРС')
    search = search_file('UMSRS')
    if search[0] and search[1]:
        send_all('файл уже сконвертирован:\n' + search[2].split('\\')[-1])
        send_all('Посмотрим что внутри...')
        check = check_file(search[2],'UMSRS')
        if check[0]:
            send_all('Файл прошёл проверку, начинаю грузить в память')
            df = pd.read_csv(search[2],header = check[3], usecols = check[2], names = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
            UMSRS_to_sql(df)
            return 1
        else:
            send_all('Файл не прошёл проверку!')
            send_all(check[1])
            return 0
    else:
        if search[0]:
            send_all('Нее... я не хочу работать с xlsx, щас конвертирую!')
            file_csv = excel_to_csv(search[2]) 
            send_all('Результат:\n' + file_csv.split('\\')[-1])
            check = check_file(file_csv,'UMSRS')
            if check[0]:
                send_all('Файл прошёл проверку, начинаю грузить в память')
                df = pd.read_csv(file_csv,header = check[3], usecols = check[2], names = check[2], na_filter = False, dtype = str, delimiter=';', engine='python')
                UMSRS_to_sql(df)
                return 1
            else:
                send_all('Файл не прошёл проверку!')
                send_all(check[1])
                return 0
        else:
            send_all('Но я не нашёл файла федерального регистра (((')
            return 0 

def medical_personal_sick():
    send_all('Начинаю считать заболевших сотрудников')
    medPers = pd.read_sql('EXEC  med.p_StartMedicalPersonalSick',conn)
    date = datetime.datetime.today().strftime("%Y_%m_%d_%H_%M")
    file = get_dir('med_sick') + r'\Заболевшие медики '+ date +'.xlsx'
    with pd.ExcelWriter(file) as writer:
        medPers.to_excel(writer,sheet_name='meducal',index=False)
    send_all("Все готово\n" + file)
    
def load_report_vp_and_cv():
	send_all('Продготовка к отчету Мониторинг ВП и COVID')
	#собираем список файлов из папки
	files = glob.glob(directory + r'\из_почты\[!~$]*.xls*') 
	path = directory + r'\\' + datetime.datetime.now().strftime("%Y%m%d")
	if os.path.exists(path):
		send_all("Директория для складирования файлов от МО %s уже существует" % path)
	else:
		try:
			os.mkdir(path)
		except OSError:
			send_all("Создать директория для складирования файлов от МО %s не удалось" % path)
		else:
			send_all("Успешно создана директория %s для сохранения файлов от МО" % path)	
		for file in files:
			try:
				load_file_mo(file)
			except:
				send_all(Не обработался следующий файл file)	
		df = pd.concat(list_)
		# переименовка столбцов		
		df = df.set_axis(['vp_03_Power_Count_Departments','vp_04_Power_Count_Allocated_All','vp_05_Power_Count_Allocated','vp_06_Power_Count_Reanimation_All','vp_07_Power_Count_Reanimation','vp_08_Hospital_All','vp_09_Hospital_Day','vp_10_Hospital_Hard_All','vp_11_Hospital_Hard_Reaniamation','vp_12_Hospital_Hard_Ivl','vp_13_ReceivedAll','vp_14_ReceivedDay','vp_15_DischargedAll','vp_16_DischargedDay','vp_17_DiedAll','vp_18_DiedDay','cv_19_Power_Count_Departments','cv_20_Power_Count_Allocated_All','cv_21_Power_Count_Allocated','cv_22_Power_Count_Reanimation_All','cv_23_Power_Count_Reanimation','cv_24_Hospital_All','cv_25_Hospital_Day','cv_26_Hospital_Hard_All','cv_27_Hospital_Hard_Reaniamation','cv_28_Hospital_Hard_Ivl','cv_29_ReceivedAll','cv_30_ReceivedDay','cv_31_DischargedAll','cv_32_DischargedDay','cv_33_DiedAll','cv_34_DiedDay','nameMO'], axis=1, inplace=False)
		send_all(Собрал свод из прочитанных файлов. Сейчас начну заливать в базу)		
		try:
			df.to_sql(
				'HistoryFileMO',
				conn,
				schema='mon_vp',
				index = False,
				if_exists='append'
				)
		except:
			send_all('Залить в базу не получилось')
		send_all('Заливка в БД прошла успешно. Сейчас что-нибудь буду выводить...')
		if check_data_table('mon_vp.v_DebtorsReport'):
			# вывод картинки с теми кто не сдал отчет
			otvet_vp('SELECT * FROM mon_vp.v_DebtorsReport') 
		else:
			send_all('Тут я должен был бы выгрузить отчет,  но я пока этого не умею делать.. учусь.')
            #otvet_vp('SELECT * FROM mon_vp.v_GrandReport')

# обработка данных в нутри файлов		
def load_file_mo(file):
	# открываем файл и пересохраняем его
    xcl = win32com.client.Dispatch("Excel.Application")
    wb = xcl.Workbooks.Open(file)
    xcl.DisplayAlerts = False
    wb.SaveAs(file)          
    xcl.Quit()
    # получаем данные из файлов  
    nameMO = pd.read_excel(file, sheet_name= 'Титул', header =3, usecols='H', nrows = 1).iloc[0,0]
    dff = pd.read_excel(file, sheet_name= 'Данные1', header =6, usecols='C:AH', nrows = 1)
    dff = dff.fillna(0)
    dff['nameMO'] = nameMO
    list_.append(dff)
    # перемещаем файл в обработанные
    os.replace(file, path + r'\\' + os.path.basename(file))   
    
