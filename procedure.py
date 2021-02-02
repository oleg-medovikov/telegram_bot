import datetime,glob,os,shutil,openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd
from loader import get_dir

def check_robot():
    date = datetime.datetime.today().strftime("%Y_%m_%d")
    path =os.getenv('path_robot') +'\\'+ date + '\\' +'*'
    spisok = 'В директории Robot сейчас лежат файлы:'
    for file in glob.glob(path):
        spisok += '\n' + file.split('\\')[-1]
    return spisok


def svod_40_COVID_19():
    path = get_dir('40_covid_19') + r'\*.xls'
    list_=[]
    usecolumns = 'A,B,C,D,F,G,I,J,L,M,O,P,R,S,U,V,X,Y,AA,AB,AC,AD,AF,AG,AI,AJ,AK'
    for xls in glob.glob(path):
        mo = pd.read_excel(xls, header=2,usecols=usecolumns,sheet_name='Для заполнения')
        mo = mo[mo[2].notnull() & (~mo[2].isin(['Пункт вакцинации'])) ]
        
        if len(mo.index) > 1:
            for i in mo.index:
                if i:
                    mo.loc[i,0] = 'Пункт вакцинации'
                    mo.loc[i,1] = mo.at[i,2].split(' ')[0]
                    mo.loc[i,2] = str(mo.at[i,2].split(' ',1)[1])
                else:
                    mo.loc[i,0] = 'Медицинская организация'

            list_.append(mo)

    if len(list_):
        df=pd.concat(list_)

        cols = list(df.columns.values)
        cols = cols[-1:] + cols[:-1]
        df =df[cols]
        df.index=range(1,len(df)+1)
#        df['Проверка 1. Вакцинировали не больше, чем V1 в МО'] = df[4] - df[20] - df[24]

        itog = pd.DataFrame()
        for col in df.columns:
            if col not in [0,1,2,24,26,27,28]:
                itog.loc[0,col] = df.loc[df[0] == 'Медицинская организация', col ].sum()

        shablon_path = os.getenv('path_help')
        new_name = datetime.datetime.today().strftime("%Y-%m-%d") + '_40_COVID_19_cvod.xlsx'

        shutil.copyfile(shablon_path + r'\40_COVID_19_svod.xlsx', shablon_path  + '\\' + new_name)

        wb= openpyxl.load_workbook( shablon_path  + '\\' + new_name)
        ws = wb['Пунты вакцинации']
        rows = dataframe_to_rows(df,index=False, header=False)

        for r_idx, row in enumerate(rows,4):  
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        wb.save( shablon_path  + '\\' + new_name) 

        wb= openpyxl.load_workbook( shablon_path  + '\\' + new_name)
        ws = wb['ИТОГО']
        rows = dataframe_to_rows(itog,index=False, header=False)

        for r_idx, row in enumerate(rows,4):  
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        wb.save( shablon_path  + '\\' + new_name) 
        return(shablon_path  + '\\' + new_name)
    else:
        return None 
