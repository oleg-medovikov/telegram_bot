import pandas as pd
import os,datetime,pyodbc,glob

from  loader import get_dir
conn=pyodbc.connect(os.getenv('sql_conn'))

class my_except(Exception):
    pass


sql_no_snils="""
select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
from cv_fedreg
where [СНИЛС] = 'Не идентифицирован'
"""
sql_bez_izhoda="""
select [УНРЗ],[Медицинская организация],[Диагноз установлен],[Дата исхода заболевания]
from dbo.cv_fedreg
where [Исход заболевания] = ''
    and DATEDIFF (day,[Диагноз установлен],getdate()) > 45
"""

sql_bez_ambulat_level ="""
select fr.* from 
(select dbo.get_Gid(idPatient) as 'Gid',[УНРЗ],[ФИО],[Дата рождения],[СНИЛС],[Вид лечения],
[МО прикрепления] as 'Медицинская организация', [Исход заболевания],[Дата исхода заболевания],[Диагноз],[Диагноз установлен]
    from dbo.cv_fedreg
        where [Медицинская организация] in (
'СПб ГБУЗ "Городская больница №40"','СПб ГБУЗ "Городская Покровская больница"','СПб ГБУЗ "ГМПБ 2"',
'СПб ГБУЗ "Городская Мариинская больница"','СПб ГБУЗ "Госпиталь для ветеранов войн"','СПб ГБУЗ "ДГКБ №5 им. Н.Ф.Филатова"',
'СПб ГБУЗ "Городская больница №38 им. Н.А.Семашко"','СПб ГБУЗ "Городская больница Святого Великомученика Георгия"',
'СПб ГКУЗ "ГПБ №3 им. И.И.Скворцова-Степанова"','СПб ГБУЗ "Александровская больница"','СПб ГБУЗ "Клиническая инфекционная больница им. С.П. Боткина"',
'ФГБУ «НМИЦ им. В.А. Алмазова» Минздрава России','ФГБОУ ВО ПСПбГМУ им. И.П.Павлова Минздрава России','СПб ГБУЗ "Городская больница №20"',
'СПб ГБУЗ "Николаевская больница"','СПб ГБУЗ "Елизаветинская больница"','ФГБУ "СЗОНКЦ им.Л.Г.Соколова ФМБА России"',
'СПб ГБУЗ "Городская больница Святого Праведного Иоанна Кронштадтского"','СПб ГБУЗ "Городская больница №15"','СПб ГБУЗ "ДГБ Св. Ольги"',
'ФГБОУ ВО СЗГМУ им. И.И. Мечникова Минздрава России','ФГБУ «НМИЦ ТО им. Р.Р. Вредена» Минздрава России',
'СПб ГБУЗ "Городская клиническая больница №31"','СПб ГБУЗ "Городская больница №26"','СПб ГБУЗ "Родильный дом №16"',
'СПб ГБУЗ "Санкт-Петербургская психиатрическая больница №1 им.П.П.Кащенко"','СПб ГБУЗ "Городская больница №33"',
'ФКУ "Санкт-Петербургская ПБСТИН" Минздрава России','СПб ГБУЗ Клиническая больница Святителя Луки','СПб ГБУЗ "Родильный дом №13"',
'ГБУ СПб НИИ СП им. И.И. Джанелидзе','ГБУЗ «СПб КНпЦСВМП(о)»','СПб ГБУЗ "Городская больница №14"',
'СПб ГБУЗ "Городской гериатрический медико-социальный центр"','СПб ГБУЗ "ДИБ №3"','СПб ГБУЗ "Детская городская больница №22"',
'СПБ ГБУЗ "Введенская больница"','СПБ ГБУЗ "ГНБ"','ФГБУ "СПб НИИФ" Минздрава России','ЧУЗ «КБ «РЖД-МЕДИЦИНА» Г. С-ПЕТЕРБУРГ"',
'СПб ГБУЗ "Городская больница №28 "Максимилиановская"','СПб ГБУЗ "Родильный дом №18"','ФГБОУ ВО СПбГПМУ Минздрава России',
'СПб ГБУЗ "Родильный дом №10"','СПб ГБУЗ "Городская больница №9"','ФГБУ "НМИЦ онкологии им. Н.Н.Петрова" Минздрава России',
'СПБ ГБУЗ "Детский городской многопрофильный клинический специализированный центр высоких медицинских технологий"','СПб ГБУЗ "Родильный дом №17"',
'СПб ГБУЗ "Городская туберкулезная больница №2"','СПб ГБУЗ "Родильный дом №9"','ФГБУ ВЦЭРМ ИМ. А.М. Никифорова МЧС России',
'СПб ГБУЗ «Клиническая инфекционная больница им.С.П.Боткина»','СПб ГБУЗ "ППТД"','СПб ГБУЗ "Клиническая ревматологическая больница №25"',
'СПб ГБУЗ "Центр СПИД и инфекционных заболеваний"','СПб ГБУЗ "ГКОД"' )
        and [Исход заболевания] in ('Перевод пациента в другую МО','Перевод пациента на амбулаторное лечение','Перевод пациента на стационарное лечение')
        and [МО прикрепления] != ''
        and  DATEDIFF (day,[Диагноз установлен],[Дата исхода заболевания])  > 7  ) as fr
    left join ( select dbo.get_Gid(idPatient) as 'Gid'  from cv_umsrs )as um 
        on (fr.Gid = um.Gid)
            where um.Gid is null
"""

sql_no_OMS="""
select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
from cv_fedreg
where [Исход заболевания] in ('', 'Перевод пациента в другую МО',
                                'Перевод пациента на амбулаторное лечение',
                                'Перевод пациента на стационарное лечение')
    and [МО прикрепления] = ''
"""

sql_neveren_vid_lechenia="""
select [УНРЗ],[ФИО],
[Дата рождения],[Медицинская организация],
[Вид лечения], [МО прикрепления], [Исход заболевания]
    from dbo.cv_fedreg
        where [Медицинская организация]  in  (
'СПб ГБУЗ "Городская поликлиника №86"','СПб ГБУЗ "Городская поликлиника №23"','СПб ГБУЗ "Городская поликлиника № 8"',
'СПб ГБУЗ "Городская поликлиника №122"','СПб ГБУЗ «Городская поликлиника № 96»','СПБ ГБУЗ "ГП № 56"','СПб ГБУЗ "Городская поликлиника №34"',
'СПб ГБУЗ "Городская поликлиника №100 Невского района Санкт-Петербурга"','СПб ГБУЗ "Городская поликлиника №94"',
'СПб ГБУЗ "Городская поликлиника №4"','СПб ГБУЗ "Городская поликлиника №44"','СПб ГБУЗ "Городская поликлиника №78"',
'СПб ГБУЗ"Поликлиника №98"','СПб ГБУЗ "Городская поликлиника №120"','СПб ГБУЗ "Городская поликлиника №49"','СПб ГБУЗ "Городская поликлиника №93"',
'СПб ГБУЗ ГП № 95','СПб ГБУЗ "Городская поликлиника №114"','СПб ГБУЗ "ГП №27"','СПб ГБУЗ "Городская поликлиника № 32"',
'СПб ГБУЗ "Городская поликлиника №72"','СПб ГБУЗ "Городская поликлиника №97"','СПб ГБУЗ "Детская городская поликлиника №68"',
'СПб ГБУЗ "Городская поликлиника №109"','СПб ГБУЗ "Городская поликлиника №14"','СПб ГБУЗ "Городская поликлиника № 117"',
'СПб ГБУЗ "Городская поликлиника №112"','СПб ГБУЗ "Городская поликлиника № 54"','СПб ГБУЗ "Городская поликлиника №106"',
'СПб ГБУЗ "Городская поликлиника №38"','СПб ГБУЗ "ДГП №29"','СПб ГБУЗ "Городская поликлиника №60 Пушкинского района"',
'СПб ГБУЗ "Городская поликлиника № 111"','СПб ГБУЗ "Поликлиника №37"','СПб ГБУЗ "Городская поликлиника №74"',
'СПб ГБУЗ "Городская поликлиника №51"','СПб ГБУЗ "Городская поликлиника №104"','СПб ГБУЗ "Городская поликлиника №3"',
'СПб ГБУЗ "Городская поликлиника №21"','СПб ГБУЗ "Городская поликлиника №19"','СПб ГБУЗ "Городская поликлиника №107"',
'СПб ГБУЗ "Городская поликлиника №46"','СПб ГБУЗ "Городская поликлиника №39"','СПб ГБУЗ "Городская поликлиника №77 Невского района"',
'СПб ГБУЗ "Городская поликлиника №48"','СПб ГБУЗ "Поликлиника №88"','СПб ГБУЗ "Городская поликлиника №102"','СПб ГБУЗ "ДГП №63"',
'СПб ГБУЗ "Городская поликлиника №91"','СПб ГБУЗ "ГП №71"','СПб ГБУЗ "Городская поликлиника №24"','СПб ГБУЗ "Городская поликлиника №25 Невского района"',
'СПб ГБУЗ "ДГП № 71"','СПб ГБУЗ "Городская поликлиника №99"','СПб ГБУЗ "Поликлиника №28"','СПб ГБУЗ "Городская поликлиника №6"',
'СПб ГБУЗ ДГП № 49','СПб ГБУЗ "Детская городская поликлиника №62"','СПб ГБУЗ "Городская поликлиника №52"','СПб ГАУЗ "Городская поликлиника №40"',
'СПБ ГБУЗ "ДГП № 44"','СПб ГБУЗ "ДГП №73"','СПб ГБУЗ "Детская городская поликлиника №11"','СПб ГБУЗ "Городская поликлиника №43"','СПб ГБУЗ "Детская городская поликлиника №35"',
'СПб ГБУЗ "Детская городская поликлиника №19"','СПб ГБУЗ "Городская поликлиника № 87"','СПб ГБУЗ "Городская поликлиника № 22"',
'СПб ГБУЗ "Детская городская поликлиника №8"','СПБ ГБУЗ "ДГП № 51"','СПб ГБУЗ ДП № 30','СПб ГБУЗ "Детская городская поликлиника №45 Невского района"',
'СПб ГБУЗ "Детская городская поликлиника №17"','СПб ГБУЗ "Детская городская поликлиника № 7"','СПб ГБУЗ "Городская поликлиника №30"',
'СПб ГБУЗ "Городская поликлиника №118"','СПб ГБУЗ «Городская поликлиника № 76»','СПб ГБУЗ "Городская поликлиника №64"','ГП № 3','ГБУЗ ГП №17'
        )
        and [Вид лечения] = 'Стационарное лечение'
        and [Исход заболевания] = ''
"""
sql_no_lab="""
select fr.[Медицинская организация], fr.[УНРЗ],fr.[ФИО],fr.[Дата рождения],fr.Диагноз
from ( select * from cv_fedreg where [Диагноз] ='U07.1') as fr
    left join (select distinct УНРЗ from [dbo].[cv_fedreg_lab] where [Результат теста (положительный/ отрицательный)] = 1 ) as lab
        on (fr.УНРЗ = lab.УНРЗ)
            where lab.УНРЗ is null
"""

sql_net_diagnoz_covid="""
select fr.[Медицинская организация], fr.[УНРЗ],fr.[ФИО],fr.[Дата рождения],fr.Диагноз
from ( select * from cv_fedreg where [Диагноз] not in ('U07.1','Z22.8') ) as fr
    inner join (select distinct УНРЗ from [dbo].[cv_fedreg_lab] where [Результат теста (положительный/ отрицательный)] = 1 ) as lab
        on (fr.УНРЗ = lab.УНРЗ)
"""

sql_net_pad="""
select [Медицинская организация], [УНРЗ], [ФИО], [Дата рождения], [Диагноз], [Исход заболевания]
from cv_fedreg 
    where [Исход заболевания] = 'Смерть'
        and [ПАД Отказ от проведения вскрытия] != 'Да'
        and [ПАД Наличие файла патологоанатомического заключения]  != 'Да'
"""
sql_net_dnevnik="""
select [Медицинская организация],[УНРЗ], [ФИО], [Дата рождения], [Дата создания РЗ]
    from cv_fedreg where [Степень тяжести] = ''
    and( [Диагноз] in ('U07.1','U07.2') or  [Диагноз]  like 'J1[2-8]%')
"""

def get_path_mo(organization):
    sql = f"select top(1) directory from robo.directory where [Наименование в ФР] = '{organization}'"
    try:
        dir = pd.read_sql(sql,conn).iat[0,0]
    except:
        return 0
    else:
        return dir

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
            file = path_otch + '\\' +str(datetime.datetime.now().date()) + ' '+ name + '.xlsx'
            with pd.ExcelWriter(file) as writer:
                part.to_excel(writer,sheet_name='унрз')
            stat.loc[k,'Статус'] = 'Файл положен'
            stat.loc[k,'Имя файла'] = file
        else:
            stat.loc[k,'Статус'] = 'Не найдена директория для файла'
    stat.index = range(1,len(stat) + 1)
    with pd.ExcelWriter(get_dir('Temp') + r'\отчет по разложению ' + name + '.xlsx') as writer:
        stat.to_excel(writer,sheet_name='унрз') 

def put_svod(df,name):
    path = get_dir('zam_svod') + '\\' + name
    name = str(datetime.datetime.now().date()) + ' ' + name + '.xlsx'
    try:
        os.mkdir(path)
    except OSError:
        pass
    df.index = range(1, len(df) + 1)
    df = df.applymap(str)
    df.fillna('пусто')
    with pd.ExcelWriter(path + '\\' + name ) as writer:
        df.to_excel(writer) 

def no_snils(a): 
    df = pd.read_sql(sql_no_snils,conn)
    put_svod(df,'Нет СНИЛСа')
    put_excel_for_mo(df,'Нет СНИЛСа')
    return get_dir('temp') + '\\' + 'отчет по разложению Нет СНИЛСа.xlsx'

def bez_izhoda(a):
    df = pd.read_sql(sql_bez_izhoda,conn) 
    put_svod(df,'Без исхода 45 дней')
    put_excel_for_mo(df,'Без исхода 45 дней')
    return get_dir('temp') + '\\' + 'отчет по разложению Без исхода 45 дней.xlsx'

def bez_ambulat_level(a):
    df = pd.read_sql(sql_bez_ambulat_level,conn) 
    put_svod(df,'Нет амбулаторного этапа')
    put_excel_for_mo(df,'Нет амбулаторного этапа')
    return get_dir('temp') + '\\' + 'отчет по разложению Нет амбулаторного этапа.xlsx'

def no_OMS(a):
    df = pd.read_sql(sql_no_OMS,conn) 
    put_svod(df,'Нет данных ОМС')
    put_excel_for_mo(df,'Нет данных ОМС')
    return get_dir('temp') + '\\' + 'отчет по разложению Нет данных ОМС.xlsx'

def neveren_vid_lechenia(a):
    df = pd.read_sql(sql_neveren_vid_lechenia,conn) 
    put_svod(df,'неверный вид лечения')
    put_excel_for_mo(df,'неверный вид лечения')
    return get_dir('temp') + '\\' + 'отчет по разложению неверный вид лечения.xlsx'

def no_lab(a):
    df = pd.read_sql(sql_no_lab,conn) 
    put_svod(df,'нет лабораторного подтверждения')
    put_excel_for_mo(df,'нет лабораторного подтверждения')
    return get_dir('temp') + '\\' + 'отчет по разложению нет лабораторного подтверждения.xlsx'

def net_diagnoz_covid(a):
    df = pd.read_sql(sql_net_diagnoz_covid,conn) 
    put_svod(df,'нет диагноза COVID')
    put_excel_for_mo(df,'нет диагноза COVID')
    return get_dir('temp') + '\\' + 'отчет по разложению нет диагноза COVID.xlsx'

def net_pad(a):
    df = pd.read_sql(sql_net_pad,conn) 
    put_svod(df,'нет ПАЗ')
    put_excel_for_mo(df,'нет ПАЗ')
    return  get_dir('temp') + '\\' + 'отчет по разложению нет ПАЗ.xlsx'

def net_dnevnik(a):
    df = pd.read_sql(sql_net_dnevnik,conn)
    put_svod(df,'нет дневниковых записей')
    put_excel_for_mo(df,'нет дневниковых записей')
    return get_dir('temp') + '\\' + 'отчет по разложению нет дневниковых записей.xlsx'

def delete_old_files(date):
    path = get_dir('covid')+ f"\\EPID.COVID.*\\EPID.COVID.*\\Замечания Мин. Здравоохранения\\{date.strftime('%Y-%m-%d')}*.xlsx"
    files = glob.glob(path)
    if len(files) == 0:
        return 'Нет файлов за это число!'
    for file in files:
        try:
            os.remove(file)
        except:
            pass
    return f"Я все поудалял за дату {date.strftime('%Y-%m-%d')}"
    