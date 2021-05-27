import pandas as pd
import os,datetime,glob,sqlalchemy

from  loader import get_dir

from sending import send

server  = os.getenv('server')
user    = os.getenv('mysqldomain') + '\\' + os.getenv('mysqluser')
passwd  = os.getenv('mypassword')
dbase   = os.getenv('db')

eng = sqlalchemy.create_engine(f"mssql+pymssql://{user}:{passwd}@{server}/{dbase}",pool_pre_ping=True)
con = eng.connect()


class my_except(Exception):
    pass

def get_path_mo(organization):
    sql = f"select top(1) [user] from robo.directory where [Наименование в ФР] = '{organization}'"
    root = get_dir('covid')
    try:
        dir = pd.read_sql(sql,con).iat[0,0]
    except:
        return 0
    else:
        return root + dir

def put_excel_for_mo(df,name):
    stat = pd.DataFrame()
    for org in df['Медицинская организация'].unique():
        k = len(stat)
        stat.loc[k,'Медицинская организация'] = org
        part = df[df['Медицинская организация'] == org ]
        part.index = range(1,len(part)+1)
        part.fillna(0, inplace = True)
        part = part.applymap(str)
        root = get_path_mo(org)
        if root:
            path_otch = root + 'Замечания Мин. Здравоохранения'
            try:
                os.makedirs(path_otch)
            except OSError:
                pass
            file = path_otch + '/' + str(datetime.datetime.now().date()) + ' ' + name + '.xlsx'
            with pd.ExcelWriter(file) as writer:
                part.to_excel(writer,sheet_name='унрз')
            stat.loc[k,'Статус'] = 'Файл положен'
            stat.loc[k,'Имя файла'] = file
        else:
            stat.loc[k,'Статус'] = 'Не найдена директория для файла'
    stat.index = range(1,len(stat) + 1)
    with pd.ExcelWriter(get_dir('temp') + '/отчёт по разложению ' + name + '.xlsx') as writer:
        stat.to_excel(writer,sheet_name='унрз') 

def put_svod(df,name): 
    path = get_dir('zam_svod') + '/' + name
    name = str(datetime.datetime.now().date()) + ' ' + name + '.xlsx'
    try:
        os.mkdir(path)
    except OSError:
        pass
    df.index = range(1, len(df) + 1)
    df = df.applymap(str)
    df.fillna('пусто')
    with pd.ExcelWriter(path + '/' + name ) as writer:
        df.to_excel(writer) 

def no_snils(a): 
    sql = open('sql/zam/no_snils.sql','r').read()
    df = pd.read_sql(sql,con)
    put_svod(df,'Нет СНИЛСа')
    put_excel_for_mo(df,'Нет СНИЛСа')
    return get_dir('temp') + '/' + 'отчёт по разложению Нет СНИЛСа.xlsx'

def bez_izhoda(a):
    sql = open('sql/zam/bez_ishod.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'Без исхода 45 дней')
    put_excel_for_mo(df,'Без исхода 45 дней')
    return get_dir('temp') + '/' + 'отчёт по разложению Без исхода 45 дней.xlsx'

def bez_ambulat_level(a):
    sql = open('sql/zam/bez_ambulat_level.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'Нет амбулаторного этапа')
    put_excel_for_mo(df,'Нет амбулаторного этапа')

    #sql = open('sql/zam/bez_ambulat_level_death_covid.sql','r').read()
    #df = pd.read_sql(sql,con) 
    #put_svod(df,'Нет амбулаторного этапа, умер от COVID')
    #put_excel_for_mo(df,'Нет амбулаторного этапа, умер от COVID')

    sql = open('sql/zam/bez_ambulat_level_death_nocovid.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'Нет амбулаторного этапа, умер не от COVID')
    put_excel_for_mo(df,'Нет амбулаторного этапа, умер не от COVID')
    temp = get_dir('temp')
    files =   temp + '/' + 'отчёт по разложению Нет амбулаторного этапа.xlsx' + ';' \
            + temp + '/' + 'отчёт по разложению Нет амбулаторного этапа, умер не от COVID.xlsx' #+ temp + '/' + 'отчёт по разложению Нет амбулаторного этапа, умер от COVID.xlsx' + ';'  
    return files

def no_OMS(a):
    sql = open('sql/zam/no_OMS.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'Нет данных ОМС')
    put_excel_for_mo(df,'Нет данных ОМС')
    return get_dir('temp') + '/' + 'отчёт по разложению Нет данных ОМС.xlsx'

def neveren_vid_lechenia(a):
    sql = open('sql/zam/neverni_vid_lecenia.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'неверный вид лечения')
    put_excel_for_mo(df,'неверный вид лечения')
    return get_dir('temp') + '/' + 'отчёт по разложению неверный вид лечения.xlsx'

def no_lab(a):
    sql = open('sql/zam/no_lab.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'нет лабораторного подтверждения')
    put_excel_for_mo(df,'нет лабораторного подтверждения')
    return get_dir('temp') + '/' + 'отчёт по разложению нет лабораторного подтверждения.xlsx'

def net_diagnoz_covid(a):
    sql = open('sql/zam/net_diagnoz_covid.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'нет диагноза COVID')
    put_excel_for_mo(df,'нет диагноза COVID')
    return get_dir('temp') + '/' + 'отчёт по разложению нет диагноза COVID.xlsx'

def net_pad(a):
    sql = open('sql/zam/net_pad.sql','r').read()
    df = pd.read_sql(sql,con) 
    put_svod(df,'нет ПАЗ')
    put_excel_for_mo(df,'нет ПАЗ')
    return  get_dir('temp') + '/' + 'отчёт по разложению нет ПАЗ.xlsx'

def net_dnevnik(a):
    sql = open('sql/zam/net_dnevnik.sql','r').read()
    df = pd.read_sql(sql,con)
    put_svod(df,'нет дневниковых записей')
    put_excel_for_mo(df,'нет дневниковых записей')
    return get_dir('temp') + '/' + 'отчёт по разложению нет дневниковых записей.xlsx'

def zavishie_statusy(a):
    sql = open('sql/zam/zavishie_status.sql','r').read()
    df = pd.read_sql(sql,con)
    put_svod(df,'зависшие статусы')
    put_excel_for_mo(df,'зависшие статусы')
    return get_dir('temp') + '/' + 'отчёт по разложению зависшие статусы.xlsx'

def delete_old_files(date):
    path = get_dir('covid')+ f"/EPID.COVID.*/EPID.COVID.*/Замечания Мин. Здравоохранения/{date.strftime('%Y-%m-%d')}*.xlsx"
    files = glob.glob(path)
    if len(files) == 0:
        return 'Нет файлов за это число!'
    for file in files:
        try:
            os.remove(file)
        except:
            pass
    return f"Я все поудалял за  дату {date.strftime('%Y-%m-%d')}"
   
def load_snils_comment(a):
    path = get_dir('snils_com') + '/*'
    text = ''
    for file in glob.glob(path):
        try:
            df = pd.read_excel(file,usecols=['УНРЗ','Примечания'])
        except Exception as e: text +=  file.split('/')[-1] +'\n' + str(e) 
        else:
            with con.connect() as cursor:
                cursor.execute("""TRUNCATE TABLE tmp.snils_comment""")
            df.to_sql('snils_comment',con,schema='tmp',if_exists='append',index=False)
            with con.connect() as cursor:
                cursor.execute("""
                update  a
                    set a.[Примечания] = b.[Примечания]
                    from [robo].[snils_comment] a inner join [tmp].[snils_comment] b
                    on a.[УНРЗ] = b.[УНРЗ]
            insert [robo].[snils_comment]
                    ([УНРЗ]
                  ,[Примечания])
                 SELECT b.[УНРЗ]
                        ,b.[Примечания]
                FROM [tmp].[snils_comment] b
                left join [robo].[snils_comment] a
                on a.[УНРЗ] = b.[УНРЗ]
                where a.[УНРЗ] is null
                """)
            text += '\n Хорошо обработан файл ' + file.split('/')[-1]
    return text

def IVL(a):
    path = get_dir('Robot') +'/'+ datetime.datetime.now().strftime("%Y_%m_%d")
    try:
        file_vp = glob.glob(path + '/Мониторинг_ВП.xlsx')[0]
    except:
        raise my_except('Не найден Мониторинг_ВП.xlsx')
    try:
        file_fr = glob.glob(path + '/*едеральный*15-00.xlsx')[0]
    except:
        raise my_except('Не найден трёхчасовой федеральный регистр')

    send('epid', 'Использую \n' + file_vp.rsplit('/',1)[-1] + '\n' + file_fr.rsplit('/',1)[-1])

    names = ['Медицинская организация','Исход заболевания','ИВЛ','Вид лечения','Субъект РФ','Диагноз']
    fr = pd.read_excel(file_fr, usecols=names,dtype=str)

    names = ['mo','vp_zan','vp_ivl','cov_zan','cov_ivl']
    vp = pd.read_excel(file_vp, usecols = "A,I,L,Y,AB",header=7, names=names)

    send('epid', 'Прочитал файлы')

    # Считаем числа в в федеральном регистре 
    S_org = ['ФГБОУ ВО СЗГМУ им. И.И. Мечникова Минздрава России','ФГБОУ ВО ПСПбГМУ им. И.П.Павлова Минздрава России',
            'СПб ГБУЗ "Городская больница 40"','СПб ГБУЗ "Городская больница 20"','СПб ГБУЗ "Николаевская больница"']

    zan1 = fr.loc[fr['Исход заболевания'].isnull() \
            & fr['Вид лечения'].isin(['Стационарное лечение']) \
            & fr['Субъект РФ'].isin(['г. Санкт-Петербург']) \
            & fr['Медицинская организация'].isin(S_org) \
            & (fr['Диагноз'].str.contains('J12.')  | fr['Диагноз'].str.contains('J18.') | fr['Диагноз'].isin(['U07.1','U07.2']) )  ]

    zan1 ['Занятые койки'] = 1 
    zan1 = zan1.groupby('Медицинская организация',as_index=False)['Занятые койки'].sum()

    zan2 = fr.loc[fr['Исход заболевания'].isnull() \
            & fr['Субъект РФ'].isin(['г. Санкт-Петербург']) \
            & ~fr['Медицинская организация'].isin(S_org) \
            & (fr['Диагноз'].str.contains('J12.')  | fr['Диагноз'].str.contains('J18.') | fr['Диагноз'].isin(['U07.1','U07.2']) )  ]

    zan2 ['Занятые койки'] = 1 
    zan2 = zan2.groupby('Медицинская организация',as_index=False)['Занятые койки'].sum()

    zan = pd.concat([zan1,zan2], ignore_index=True)

    ivl1 = fr.loc[fr['Исход заболевания'].isnull() \
            & fr['ИВЛ'].notnull() \
            & fr['Вид лечения'].isin(['Стационарное лечение']) \
            & fr['Медицинская организация'].isin(S_org) ]

    ivl1 ['ИВЛ'] = 1
    ivl1 = ivl1.groupby('Медицинская организация',as_index=False)['ИВЛ'].sum()

    ivl2 = fr.loc[fr['Исход заболевания'].isnull() \
            & fr['ИВЛ'].notnull() \
            & ~fr['Медицинская организация'].isin(S_org) ]

    ivl2 ['ИВЛ'] = 1
    ivl2 = ivl2.groupby('Медицинская организация',as_index=False)['ИВЛ'].sum()

    ivl = pd.concat([ivl1,ivl2], ignore_index=True)

    df = zan.merge(ivl,how='outer')

    df = df.fillna(0)

    change_mo =[
	[r'Больница 20 Поликлиническое отделение 42 (площадка Ленсовета)',r'СПб ГБУЗ "Городская больница 20"']
	,[r'СПб ГБУЗ "Городская больница 40" площадка Пансионат "Заря"',r'СПб ГБУЗ "Городская больница 40"']
	,[r'СПб ГБУЗ "Городская Александровская больница"',r'СПб ГБУЗ "Александровская больница"']
	,[r'СПб ГБУЗ "Клиническая больница Святителя Луки"',r'СПб ГБУЗ Клиническая больница Святителя Луки']
	,[r'СПб ГБУЗ "Городская больница Святой преподобномученицы Елизаветы"',r'СПб ГБУЗ "Елизаветинская больница"']
	,[r'СПб ГБУЗ "Детская городская больница Святой Ольги"',r'СПб ГБУЗ "ДГБ Св. Ольги"']
	,[r'СПб ГБУЗ "Детская городская клиническая больница 5 имени Нила Федоровича Филатова"',r'СПб ГБУЗ "ДГКБ 5 им. Н.Ф.Филатова"']
	,[r'СПб ГКУЗ "Городская психиатрическая больница 3 им.И.И.Скворцова-Степанова"',r'СПб ГКУЗ "ГПБ 3 им. И.И.Скворцова-Степанова"']
	,[r'СПб ГБУЗ "ДГМКЦ ВМТ им.К.А.Раухфуса"',r'СПБ ГБУЗ ДГМКЦ ВМТ им. К.А.Раухфуса']
	,[r'СПб ГБУЗ "Центр по профилактике и борьбе со СПИД и инфекционными заболеваниями"',r'СПб ГБУЗ "Центр СПИД и инфекционных заболеваний"']
	,[r'ФГБОУ ВО ПСПбГМУ им. И.П.Павлова" Минздрава России',r'ФГБОУ ВО ПСПбГМУ им. И.П.Павлова Минздрава России']
	,[r'ФГБОУ ВО СЗГМУ им.И.И.Мечникова Минздрава России',r'ФГБОУ ВО СЗГМУ им. И.И. Мечникова Минздрава России']
	,[r'ФГБУЗ Клиническая больница 122 им.Л.Г.Соколова ФМБА России',r'ФГБУ "СЗОНКЦ им.Л.Г.Соколова ФМБА России"']
	,[r'СПб ГБУЗ "Введенская больница"',r'СПБ ГБУЗ "Введенская больница"']
	,[r'СПб ГБУЗ "Психиатрическая больница 1 им.П.П.Кащенко"',r'СПб ГБУЗ "Санкт-Петербургская психиатрическая больница 1 им.П.П.Кащенко"']
	,[r'ФГБОУ ВО "Санкт-Петербургский Государственный педиатрический медицинский университет Минздрава России"',r'ФГБОУ ВО СПбГПМУ Минздрава России']
	,[r'СПб ГБУЗ "Госпиталь для ветеранов войн" площадка "ЛЕНЭКСПО"',r'СПб ГБУЗ "Госпиталь для ветеранов войн"']
	,[r'ФГБОУ ВО СПБГПМУ МИНЗДРАВА РОССИИ',r'ФГБОУ ВО СПбГПМУ Минздрава России']
	,[r'СПб ГБУЗ "Детская городская клиническая больница 5 имени Нила Федоровича Филатова"',r'СПб ГБУЗ "ДГКБ 5 им. Н.Ф.Филатова"']
	,[r'СПб ГБУЗ "Городская многопрофильная больница 2"' , r'СПб ГБУЗ "ГМПБ 2"']
               ]
    vp = vp.fillna(0)
    # Меняем названия МО и сумируем строки 
    zamena = 'произошла замена строки '
    for i in range(len(vp)):
        vp.loc[i,'zan'] = vp.at[i,'vp_zan'] + vp.at[i,'cov_zan']
        vp.loc[i,'ivl'] = vp.at[i,'vp_ivl'] + vp.at[i,'cov_ivl']
        for bad,good in change_mo:
            if vp.loc[i,'mo'] == bad:
                vp.loc[i,'mo'] = good
                zamena += '\n ' + bad + ' на ' + good

    send('epid',zamena)
    vp = vp.groupby('mo',as_index=False)['zan','ivl'].sum()
    vp.index = range(len(vp))

   # получаем отчёт по ИВЛ
    ivl_otchet = vp.merge(df, left_on='mo',right_on='Медицинская организация',how='left')
    del ivl_otchet ['mo']
    del ivl_otchet ['Занятые койки']
    del ivl_otchet ['zan']
    ivl_otchet = ivl_otchet.fillna(0)


    for i in range(len(ivl_otchet)):
        ivl_otchet.loc[i,'Разница'] = ivl_otchet.at[i,'ИВЛ'] - ivl_otchet.at[i,'ivl']

    ivl_otchet.rename(columns = { 'ivl':'ИВЛ из ежедневного отчёта','ИВЛ':'ИВЛ из Фед Регистра'}, inplace = True)

    columnsTitles=['Медицинская организация','ИВЛ из ежедневного отчёта','ИВЛ из Фед Регистра','Разница']
    ivl_otchet = ivl_otchet[ivl_otchet['Медицинская организация'] != 0 ]
    ivl_otchet=ivl_otchet.reindex(columns=columnsTitles)

    ivl_otchet.index = range(1,len(ivl_otchet)+1)

    medorg_ivl_otchet = ivl_otchet['Медицинская организация'].unique()

    # получаем отчёт по койкам
    zan_otchet = vp.merge(df, left_on='mo',right_on='Медицинская организация',how='left')

    del zan_otchet ['mo']
    del zan_otchet ['ИВЛ']
    del zan_otchet['ivl']
    zan_otchet = zan_otchet.fillna(0)

    for i in range(len(zan_otchet)):
        zan_otchet.loc[i,'Разница'] = zan_otchet.at[i,'Занятые койки'] - zan_otchet.at[i,'zan']

    zan_otchet.rename(columns = {'zan':'Заняты койки из ежедневного отчёта','Занятые койки':'Койки из Фед Регистра'}, inplace = True)


    columnsTitles=['Медицинская организация','Заняты койки из ежедневного отчёта','Койки из Фед Регистра','Разница']
    zan_otchet=zan_otchet.reindex(columns=columnsTitles)
    zan_otchet = zan_otchet[zan_otchet['Медицинская организация'] != 0 ]
    zan_otchet.index=range(1,len(zan_otchet)+1)

    medorg_zan_otchet = zan_otchet['Медицинская организация'].unique()

    path_otc ='/mnt/COVID-списки/Замечания Мин. Здравоохранения (Своды)/сверка ИВЛ и занятые койки'

    with pd.ExcelWriter(path_otc+ '/' + str(datetime.datetime.now().date()) + ' пациенты на ИВЛ новый.xlsx') as writer:
        ivl_otchet.to_excel(writer,sheet_name='ИВЛ')
        zan_otchet.to_excel(writer,sheet_name='занятые койки')
    
    put_excel_for_mo(ivl_otchet,'Пациенты на ИВЛ')
    put_excel_for_mo(zan_otchet,'Занятые койки')
    
    return get_dir('temp') + '/' + 'отчёт по разложению Пациенты на ИВЛ.xlsx' +';'+ get_dir('temp') + '/' + 'отчёт по разложению Занятые койки.xlsx'
