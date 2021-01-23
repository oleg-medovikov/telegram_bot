import os,pyodbc,subprocess
import pandas as pd

conn = pyodbc.connect(os.getenv('sql_conn'))

def fr_deti():
    sql_week = 'exec [dbo].[Proc_Report_Children_by_week]'
    sql_month = 'exec [dbo].[Proc_Report_Children_by_month]'
    file = r'C:\Users\MedovikovOE\Documents\Python_Scripts\Temp\otchet_deti.xlsx'
    week = pd.read_sql(sql_week,conn)
    month = pd.read_sql(sql_month,conn)
    with pd.ExcelWriter(file) as writer:
        week.to_excel(writer,sheet_name='week',index=False) 
        month.to_excel(writer,sheet_name='month',index=False) 
    return file


def fr_status():
    df = pd.read_sql('SELECT * FROM robo.fr_status',conn)
    table_html = r'C:\Users\MedovikovOE\Documents\Python_Scripts\Temp\table.html'
    table_png = r'C:\Users\MedovikovOE\Documents\Python_Scripts\Temp\table.png'
    df.to_html(table_html)
    subprocess.call('wkhtmltoimage.exe --encoding utf-8 -f png --width 0 ' +  table_html + ' ' + table_png, shell=True)
    return table_png

