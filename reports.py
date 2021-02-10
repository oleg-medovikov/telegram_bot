import os,pyodbc,subprocess,datetime
import pandas as pd
from sqlalchemy import *

conn = pyodbc.connect(os.getenv('sql_conn'))
con = create_engine(os.getenv('sql_engine'),convert_unicode=False)

def get_dir(name):
    sql = f"SELECT Directory FROM [robo].[directions_for_bot] where NameDir = '{name}'"
    df = pd.read_sql(sql,con)
    return df.iloc[0,0] 

def sql_execute(sql):
    Session = sessionmaker(bind=con)
    session = Session()
    session.execute(sql)
    session.commit()
    session.close()


def fr_deti():
    sql_week = 'exec [dbo].[Proc_Report_Children_by_week]'
    sql_month = 'exec [dbo].[Proc_Report_Children_by_month]'
    file = get_dir('temp') + r'\otchet_deti.xlsx'
    week = pd.read_sql(sql_week,conn)
    month = pd.read_sql(sql_month,conn)
    with pd.ExcelWriter(file) as writer:
        week.to_excel(writer,sheet_name='week',index=False) 
        month.to_excel(writer,sheet_name='month',index=False) 
    return file


def short_report(textsql):
    df = pd.read_sql(textsql,conn)
    table_html = get_dir('temp') + r'\table.html'
    table_png = get_dir('temp') + r'\table.png'
    df.to_html(table_html)
    subprocess.call('wkhtmltoimage.exe --encoding utf-8 -f png --width 0 ' +  table_html + ' ' + table_png, shell=True)
    return table_png

def dead_not_mss(a):
    frNotMss = pd.read_sql('EXEC dbo.p_FedRegNotMss',conn)
    date = datetime.datetime.today().strftime("%Y_%m_%d_%H_%M")
    file = get_dir('fr_not_mss') + r'\Список_без_МСС_'+ date +'.xlsx'
    with pd.ExcelWriter(file) as writer:
        frNotMss.to_excel(writer,sheet_name='notMSS',index=False)
    return "Список готов\n" + file.split('\\')[-1]

def dynamics(a):
    frNotMss = pd.read_sql('EXEC dbo.275_M3',conn)
    date = datetime.datetime.today().strftime("%Y_%m_%d_%H_%M")
    file = get_dir('dynam') + r'\Динамика_'+ date +'.xlsx'
    with pd.ExcelWriter(file) as writer:
        frNotMss.to_excel(writer,sheet_name='notMSS',index=False)
    return "Динамика готова\n" + file.split('\\')[-1]
