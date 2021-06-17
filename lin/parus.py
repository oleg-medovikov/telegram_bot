import sqlalchemy,cx_Oracle,os,openpyxl,shutil,datetime,subprocess,time,imgkit
from openpyxl.utils.dataframe import dataframe_to_rows
import numpy as np
import pandas as pd
from loader import get_dir
from sending import send

base_parus = os.getenv('base_parus')
userName = os.getenv('oracle_user')
password = os.getenv('oracle_pass')
userbase = os.getenv('oracle_base')

class my_except(Exception):
    pass

def o_40_covid_by_date(a):
    sql = open('sql/parus/covid_40_by_date.sql', 'r').read()    
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    df = df.loc[df.TVSP.notnull()]
    df = df.loc[~df.TVSP.isin(['Пункт вакцинации'])]
    df.V_1 = pd.to_numeric(df.V_1)
    df.V_2 = pd.to_numeric(df.V_2)
    df.DAY = df.DAY.dt.date
    res = df.pivot_table(index=['DIST','TVSP'], columns=['DAY'], values=['V_1','V_2'],fill_value=0, aggfunc=np.sum)
    summ = df.pivot_table(index=['DIST'], columns=['DAY'], values=['V_1','V_2'],fill_value=0, aggfunc=np.sum)
    total = pd.DataFrame(summ.sum()).T

    res['Район'] = res.index.get_level_values(0)
    res['Пункт вакцинации'] = res.index.get_level_values(1)

    summ['Район'] = summ.index.get_level_values(0)
    summ['Пункт вакцинации'] = 'Всего'
    total['Район'] = 'Весь город'
    total['Пункт вакцинации'] = 'Все'

    itog= pd.concat([total,summ,res], ignore_index=True)
    itog = itog.set_index(['Район','Пункт вакцинации'])
    
    file = get_dir('temp') + '/Вакцинация по датам.xlsx'
    with pd.ExcelWriter(file) as writer:
        itog.V_1.to_excel(writer,sheet_name='Первый компонент вакцины')
        itog.V_2.to_excel(writer,sheet_name='Второй компонент вакцины')
    return file 

def svod_40_cov_19(a):
    sql  = open('sql/parus/covid_40_spytnic.sql','r').read()
    sql2 = open('sql/parus/covid_40_spytnic_old.sql','r').read()
    sql3 = open('sql/parus/covid_40_epivak.sql','r').read()
    sql4 = open('sql/parus/covid_40_epivak_old.sql','r').read()
    sql5 = open('sql/parus/covid_40_covivak.sql','r').read()
    sql6 = open('sql/parus/covid_40_covivak_old.sql','r').read()
    
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        sput = pd.read_sql(sql,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        sput_old = pd.read_sql(sql2,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        epivak = pd.read_sql(sql3,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        epivak_old = pd.read_sql(sql4,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        covivak = pd.read_sql(sql5,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        covivak_old = pd.read_sql(sql6,con)
    
    send('', 'Запросы к базе выполнены')
    del sput ['ORGANIZATION']
    del sput_old ['ORGANIZATION']
    del epivak ['ORGANIZATION']
    del epivak_old ['ORGANIZATION']
    del covivak ['ORGANIZATION']
    del covivak_old ['ORGANIZATION']
    
    date_otch = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d.%m.%Y')
    new_name_pred ='40_COVID_19_БОТКИНА_' + date_otch + '_предварительный.xlsx'
    new_name_osn  ='40_COVID_19_БОТКИНА_' + date_otch + '_основной.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/40_COVID_19_pred.xlsx' , shablon_path  + '/' + new_name_pred)
    shutil.copyfile(shablon_path + '/40_COVID_19_osn.xlsx'  , shablon_path  + '/' + new_name_osn)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name_pred)
    ws = wb['Спутник-V']
    rows = dataframe_to_rows(sput,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    ws = wb['Вчера_Спутник']
    rows = dataframe_to_rows(sput_old,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    ws = wb['ЭпиВакКорона']
    rows = dataframe_to_rows(epivak,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    
    ws = wb['Вчера_ЭпиВак']
    rows = dataframe_to_rows(epivak_old,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    ws = wb['КовиВак']
    rows = dataframe_to_rows(covivak,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    ws = wb['Вчера_КовиВак']
    rows = dataframe_to_rows(covivak_old,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name_pred) 

    send('', 'Готов предварительный файл')

    # основной отчёт
    del sput[sput.columns[-1]]
    del sput[sput.columns[-1]]
    del sput[sput.columns[-1]]

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name_osn)
    ws = wb['Спутник-V']
    rows = dataframe_to_rows(sput,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    del epivak[epivak.columns[-1]]
    del epivak[epivak.columns[-1]]
    del epivak[epivak.columns[-1]]
    
    ws = wb['ЭпиВакКорона']
    rows = dataframe_to_rows(epivak,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    del covivak[covivak.columns[-1]]
    del covivak[covivak.columns[-1]]
    del covivak[covivak.columns[-1]]

    ws = wb['КовиВак']
    rows = dataframe_to_rows(covivak,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name_osn) 

    return(shablon_path  + '/' + new_name_pred + ';' + shablon_path  + '/' + new_name_osn)

def svod_50_cov_19(a):
    sql1  = open('sql/parus/covid_50_polic.sql','r').read()
        
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        polic = pd.read_sql(sql1,con)
    
    send('', 'Запросы к базе выполнены')
    date_otch = polic['DAY'].unique()[0]
    
    del polic ['DAY']

    new_name_osn  ='50_COVID_19_' + date_otch + '_предварительный.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/40_COVID_19_pred.xlsx' , shablon_path  + '/' + new_name_pred)
    shutil.copyfile(shablon_path + '/40_COVID_19_osn.xlsx'  , shablon_path  + '/' + new_name_osn)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name_pred)
    ws = wb['Разрез по МО(поликлиники)']
    rows = dataframe_to_rows(sput,index=False, header=False)
    for r_idx, row in enumerate(rows,7):  
        for c_idx, value in enumerate([2,9,10,11,12,13,14,15,16,17,18,19,20,21]):
            ws.cell(row=r_idx, column=c_idx, value=value)
    
    wb.save( shablon_path  + '/' + new_name_pred) 
    return shablon_path  + '/' + new_name_pred

def parus_43_cov_nulls(a):
    sql=open('sql/parus/covid_43_nulls.sql','r').read()
    
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    df = df.fillna(0)
    table_html = get_dir('temp') + '/table.html'
    table_png  = get_dir('temp') + '/table.png'

    df.to_html(table_html,justify='center', index=False)

    command = "/usr/bin/wkhtmltoimage --encoding utf-8 -f png --width 0 " +  table_html + " " + table_png
    try:
        subprocess.check_call(command,  shell=True)
    except:
        raise my_except('Не удалось сделать файл\n' +  subprocess.check_output(command,  shell=True))
    else:
        return table_png

def svod_43_covid_19(a):
    sql = open('sql/parus/covid_43_svod.sql','r').read()
    if datetime.datetime.today().weekday() == 0:
        sql1 = open('sql/parus/covid_43_svod_old3.sql','r').read()
    else:
        sql1 = open('sql/parus/covid_43_svod_old1.sql','r').read()
    
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)
        df_old = pd.read_sql(sql1,con)
    
    df.index     = range(1,len(df) + 1)
    df_old.index = range(1,len(df_old) + 1)

    date_otch = datetime.datetime.now().strftime('%d_%m_%Y')
    new_name = date_otch + '_43_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')
    shutil.copyfile(shablon_path + '/43 COVID 19.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Разрез по МО']
    rows = dataframe_to_rows(df,index=True, header=False)
    for r_idx, row in enumerate(rows,4):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name) 
    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Вчера']
    rows = dataframe_to_rows(df_old,index=True, header=False)
    for r_idx, row in enumerate(rows,4):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def no_save_43(a):
    sql= open('sql/parus/covid_43_no_save.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    df = df.fillna(0)
    table_html = get_dir('temp') + '/table.html'
    table_png  = get_dir('temp') + '/table.png'

    df.to_html(table_html,justify='center', index=False)
    command = "/usr/bin/wkhtmltoimage --encoding utf-8 -f png --width 0 " +  table_html + " " + table_png
    try:
        subprocess.check_call(command,  shell=True)
    except:
        raise my_except('Не удалось сделать файл\n' +  subprocess.check_output(command,  shell=True))
    else:
        return table_png

def no_save_50(a):
    sql= open('sql/parus/covid_50_no_save.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    df = df.fillna(0)
    table_html = get_dir('temp') + '/table.html'
    table_png  = get_dir('temp') + '/table.png'

    df.to_html(table_html,justify='center', index=False)
    command = "/usr/bin/wkhtmltoimage --encoding utf-8 -f png --width 0 " +  table_html + " " + table_png
    try:
        subprocess.check_call(command,  shell=True)
    except:
        raise my_except('Не удалось сделать файл\n' +  subprocess.check_output(command,  shell=True))
    else:
        return table_png

def cvod_26_covid(a):
    sql = open('sql/parus/covid_26_svod.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d_%m_%Y')
    new_name = date + '_26_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/26_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Для заполнения']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 2):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def cvod_27_covid(a):
    sql = open('sql/parus/covid_27_svod.sql','r').read()
    
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    date = datetime.datetime.now().strftime('%d_%m_%Y')
    new_name = date + '_27_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/27_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)


    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Для заполнения']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,4):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def cvod_29_covid(a): 
    if int(time.strftime("%H")) < 16:
        sql = open('sql/parus/covid_29_svod1.sql','r').read()
        date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d_%m_%Y')
    else:
        sql = open('sql/parus/covid_29_svod0.sql','r').read()
        date = datetime.datetime.now().strftime('%d_%m_%Y')
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    new_name = date + '_29_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/29_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Для заполнения']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,3):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def cvod_33_covid(a):
    sql = open('sql/parus/covid_33_svod.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d_%m_%Y')
    new_name = date + '_33_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/33_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Свод']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 2):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def cvod_36_covid(a):
    sql = open('sql/parus/covid_36_svod.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    date = df['DAY'].unique()[0]
    del df['DAY']
    new_name = date + '_36_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/36_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Свод']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,7):  
        for c_idx, value in enumerate(row, 2):
             ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def cvod_37_covid(a):
    sql = open('sql/parus/covid_37_svod.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    date = df['DAY'].unique()[0]
    del df['DAY']
    new_name = date + '_37_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/37_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Свод']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,6):  
        for c_idx, value in enumerate(row, 2):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

def cvod_38_covid(a):
    sql = open('sql/parus/covid_37_svod.sql','r').read()
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)

    date = df['DAY'].unique()[0]
    del df['DAY']
    new_name = date + '_38_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/38_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Свод']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,8):  
        for c_idx, value in enumerate(row, 2):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name)
    return  shablon_path  + '/' + new_name

