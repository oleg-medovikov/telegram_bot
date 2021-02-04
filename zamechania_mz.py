import pandas as pd
import os,datetime,pyodbc,glob

from loader import get_dir
conn=pyodbc.connect(os.getenv('sql_conn'))


sql_no_snils="""
select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
from cv_fedreg
where [СНИЛС] = 'Не идентифицирован'
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
    with pd.ExcelWriter(get_dir('Temp') + '\отчет по разложению ' + name + '.xlsx') as writer:
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
    with pd.ExcelWriter(path + '\\' + name + '.xlsx') as writer:
        df.to_excel(writer)


def no_snils():
    df = pd.read_sql(sql_no_snils,conn)
    put_svod(df,'Нет СНИЛСа')
    put_excel_for_mo(df,'Нет СНИЛСа')
    return 1

