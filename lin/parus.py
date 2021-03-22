import sqlalchemy,cx_Oracle,os,openpyxl,shutil,datetime
from openpyxl.utils.dataframe import dataframe_to_rows
import numpy as np
import pandas as pd
from loader import get_dir

base_parus = os.getenv('base_parus')

eng = sqlalchemy.create_engine("oracle+cx_oracle://" + base_parus + "/spb", pool_recycle=3600)
con = eng.connect()

class my_except():
    pass

def o_40_covid_by_date(a):
    sql = """
        SELECT day, substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,  tvsp, V_1, V_2  FROM (
        SELECT 
                r.BDATE day,
                a.AGNNAME organization,
            ro.NUMB row_index ,
            i.CODE pokazatel,
                CASE WHEN STRVAL  IS NOT NULL THEN STRVAL 
                         WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
                         WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
                        ELSE NULL END value
        FROM PARUS.BLTBLVALUES v
        INNER JOIN PARUS.BLTABLESIND si 
        on(v.BLTABLESIND = si.RN)
        INNER JOIN PARUS.BALANCEINDEXES i 
        on(si.BALANCEINDEXES = i.RN)
        INNER JOIN PARUS.BLTBLROWS ro 
        on(v.PRN = ro.RN)
        INNER JOIN PARUS.BLSUBREPORTS s 
        on(ro.PRN = s.RN)
        INNER JOIN PARUS.BLREPORTS r 
        on(s.PRN = r.RN)
        INNER JOIN PARUS.AGNLIST a
        on(r.AGENT = a.RN)
        INNER JOIN PARUS.BLREPFORMED rd
        on(r.BLREPFORMED = rd.RN)
        INNER JOIN PARUS.BLREPFORM rf
        on(rd.PRN = rf.RN)
        WHERE rf.code = '40 COVID 19'
        and r.BDATE > TO_DATE('30-01-2021','DD-MM-YYYY') 
        and i.CODE in ('Vaccin_TVSP','Vaccin_tvsp_18', 'Vaccin_tvsp_19_day' )
        )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_tvsp_18' V_1, 'Vaccin_tvsp_19_day' V_2 )
        )
        ORDER BY day, ROW_INDEX"""

    try:
        df = pd.read_sql(sql,con)
    except:
        raise my_except('Соединение с базой паруса дурит')
    
    df = df.loc[df.tvsp.notnull()]
    df = df.loc[~df.tvsp.isin(['Пункт вакцинации'])]
    df.v_1 = pd.to_numeric(df.v_1)
    df.v_2 = pd.to_numeric(df.v_2)
    df.day = df.day.dt.date
    res = df.pivot_table(index=['dist','tvsp'], columns=['day'], values=['v_1','v_2'],fill_value=0, aggfunc=np.sum)
    summ = df.pivot_table(index=['dist'], columns=['day'], values=['v_1','v_2'],fill_value=0, aggfunc=np.sum)
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
        itog.v_1.to_excel(writer,sheet_name='Первый компонент вакцины')
        itog.v_2.to_excel(writer,sheet_name='Второй компонент вакцины')
    return file 

def svod_40_cov_19(a):
    sql = """SELECT     case when tvsp is null then 'Медицинская организация' else 'Пункт вакцинации' end type, 
                        substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,
                        case when tvsp is null then organization else tvsp end tvsp, 
                        CAST(Vaccin_tvsp_03 AS int) Vaccin_tvsp_03 , CAST(Vaccin_tvsp_04 AS int) Vaccin_tvsp_04, 
                        CAST(Vaccin_tvsp_04_day AS int) Vaccin_tvsp_04_day, CAST(Vaccin_tvsp_05 AS int) Vaccin_tvsp_05,
                        CAST(Vaccin_tvsp_06 AS int) Vaccin_tvsp_06, CAST(Vaccin_tvsp_07 AS int) Vaccin_tvsp_07, CAST(Vaccin_tvsp_08 AS int) Vaccin_tvsp_08,
                        CAST(Vaccin_tvsp_09 AS int) Vaccin_tvsp_09, CAST(Vaccin_tvsp_10 AS int) Vaccin_tvsp_10,
                        CAST(Vaccin_tvsp_11 AS int) Vaccin_tvsp_11, CAST(Vaccin_tvsp_12 AS int) Vaccin_tvsp_12, 
                        CAST(Vaccin_tvsp_20 AS int) Vaccin_tvsp_20, CAST(Vaccin_tvsp_20_day AS int) Vaccin_tvsp_20_day,
                        CAST(Vaccin_tvsp_13 AS int) Vaccin_tvsp_13, CAST(Vaccin_tvsp_14 AS int) Vaccin_tvsp_14, CAST(Vaccin_tvsp_15 AS int) Vaccin_tvsp_15,
                        CAST(Vaccin_tvsp_16 AS int) Vaccin_tvsp_16, CAST(Vaccin_tvsp_17 AS int) Vaccin_tvsp_17,
                        CAST(Vaccin_tvsp_18 AS int) Vaccin_tvsp_18, CAST(Vaccin_tvsp_19 AS int) Vaccin_tvsp_19, CAST(Vaccin_tvsp_19_day AS int) Vaccin_tvsp_19_day,
                        CAST(Vaccin_tvsp_21 AS int) Vaccin_tvsp_21, CAST(Vaccin_tvsp_21_day AS int) Vaccin_tvsp_21_day, CAST(Vaccin_tvsp_23 AS int) Vaccin_tvsp_23 
        FROM (
        SELECT
                r.BDATE day,
                a.AGNNAME organization,
            i.CODE pokazatel,
            ro.NUMB row_index ,
                CASE WHEN STRVAL  IS NOT NULL THEN STRVAL
                         WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
                         WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
                        ELSE NULL END value
        FROM PARUS.BLTBLVALUES v
        INNER JOIN PARUS.BLTABLESIND si
        on(v.BLTABLESIND = si.RN)
        INNER JOIN PARUS.BALANCEINDEXES i
        on(si.BALANCEINDEXES = i.RN)
        INNER JOIN PARUS.BLTBLROWS ro
        on(v.PRN = ro.RN)
        INNER JOIN PARUS.BLSUBREPORTS s
        on(ro.PRN = s.RN)
        INNER JOIN PARUS.BLREPORTS r
        on(s.PRN = r.RN)
        INNER JOIN PARUS.AGNLIST a
        on(r.AGENT = a.RN)
        INNER JOIN PARUS.BLREPFORMED rd
        on(r.BLREPFORMED = rd.RN)
        INNER JOIN PARUS.BLREPFORM rf
        on(rd.PRN = rf.RN)
        WHERE rf.code = '40 COVID 19'
        and r.BDATE =  trunc(SYSDATE-1)
        and i.CODE in ('Vaccin_TVSP','Vaccin_tvsp_03','Vaccin_tvsp_04','Vaccin_tvsp_04_day','Vaccin_tvsp_05',
                                        'Vaccin_tvsp_06','Vaccin_tvsp_07','Vaccin_tvsp_08', 'Vaccin_tvsp_09', 'Vaccin_tvsp_10',
                                        'Vaccin_tvsp_11', 'Vaccin_tvsp_12', 'Vaccin_tvsp_20','Vaccin_tvsp_20_day',
                                        'Vaccin_tvsp_13','Vaccin_tvsp_14','Vaccin_tvsp_15', 'Vaccin_tvsp_16', 'Vaccin_tvsp_17',
                                        'Vaccin_tvsp_18', 'Vaccin_tvsp_19', 'Vaccin_tvsp_19_day','Vaccin_tvsp_21', 'Vaccin_tvsp_21_day'
                                        , 'Vaccin_tvsp_23')
        )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_tvsp_03' Vaccin_tvsp_03,'Vaccin_tvsp_04' Vaccin_tvsp_04,'Vaccin_tvsp_04_day' Vaccin_tvsp_04_day,
                                        'Vaccin_tvsp_05' Vaccin_tvsp_05, 'Vaccin_tvsp_06' Vaccin_tvsp_06,'Vaccin_tvsp_07' Vaccin_tvsp_07,
                                        'Vaccin_tvsp_08' Vaccin_tvsp_08, 'Vaccin_tvsp_09' Vaccin_tvsp_09, 'Vaccin_tvsp_10' Vaccin_tvsp_10,
                                        'Vaccin_tvsp_11' Vaccin_tvsp_11, 'Vaccin_tvsp_12' Vaccin_tvsp_12, 'Vaccin_tvsp_20' Vaccin_tvsp_20,
                                        'Vaccin_tvsp_20_day' Vaccin_tvsp_20_day,'Vaccin_tvsp_13' Vaccin_tvsp_13,'Vaccin_tvsp_14' Vaccin_tvsp_14,
                                        'Vaccin_tvsp_15' Vaccin_tvsp_15, 'Vaccin_tvsp_16' Vaccin_tvsp_16, 'Vaccin_tvsp_17' Vaccin_tvsp_17,
                                        'Vaccin_tvsp_18' Vaccin_tvsp_18, 'Vaccin_tvsp_19' Vaccin_tvsp_19, 'Vaccin_tvsp_19_day' Vaccin_tvsp_19_day,
                                        'Vaccin_tvsp_21' Vaccin_tvsp_21, 'Vaccin_tvsp_21_day' Vaccin_tvsp_21_day, 'Vaccin_tvsp_23' Vaccin_tvsp_23)
        )
        ORDER BY ROW_INDEX"""

    sql2 = """SELECT     case when tvsp is null then 'Медицинская организация' else 'Пункт вакцинации' end type, 
                        substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,
                        case when tvsp is null then organization else tvsp end tvsp,
                        CAST(Vaccin_tvsp_03 AS int) Vaccin_tvsp_03 , CAST(Vaccin_tvsp_04 AS int) Vaccin_tvsp_04, 
                        CAST(Vaccin_tvsp_04_day AS int) Vaccin_tvsp_04_day, CAST(Vaccin_tvsp_05 AS int) Vaccin_tvsp_05,
                        CAST(Vaccin_tvsp_06 AS int) Vaccin_tvsp_06, CAST(Vaccin_tvsp_07 AS int) Vaccin_tvsp_07, CAST(Vaccin_tvsp_08 AS int) Vaccin_tvsp_08,
                        CAST(Vaccin_tvsp_09 AS int) Vaccin_tvsp_09, CAST(Vaccin_tvsp_10 AS int) Vaccin_tvsp_10,
                        CAST(Vaccin_tvsp_11 AS int) Vaccin_tvsp_11, CAST(Vaccin_tvsp_12 AS int) Vaccin_tvsp_12, 
                        CAST(Vaccin_tvsp_20 AS int) Vaccin_tvsp_20, CAST(Vaccin_tvsp_20_day AS int) Vaccin_tvsp_20_day,
                        CAST(Vaccin_tvsp_13 AS int) Vaccin_tvsp_13, CAST(Vaccin_tvsp_14 AS int) Vaccin_tvsp_14, CAST(Vaccin_tvsp_15 AS int) Vaccin_tvsp_15,
                        CAST(Vaccin_tvsp_16 AS int) Vaccin_tvsp_16, CAST(Vaccin_tvsp_17 AS int) Vaccin_tvsp_17,
                        CAST(Vaccin_tvsp_18 AS int) Vaccin_tvsp_18, CAST(Vaccin_tvsp_19 AS int) Vaccin_tvsp_19, CAST(Vaccin_tvsp_19_day AS int) Vaccin_tvsp_19_day,
                        CAST(Vaccin_tvsp_21 AS int) Vaccin_tvsp_21, CAST(Vaccin_tvsp_21_day AS int) Vaccin_tvsp_21_day, CAST(Vaccin_tvsp_23 AS int) Vaccin_tvsp_23 
        FROM (
        SELECT
                r.BDATE day,
                a.AGNNAME organization,
            i.CODE pokazatel,
            ro.NUMB row_index ,
                CASE WHEN STRVAL  IS NOT NULL THEN STRVAL
                         WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
                         WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
                        ELSE NULL END value
        FROM PARUS.BLTBLVALUES v
        INNER JOIN PARUS.BLTABLESIND si
        on(v.BLTABLESIND = si.RN)
        INNER JOIN PARUS.BALANCEINDEXES i
        on(si.BALANCEINDEXES = i.RN)
        INNER JOIN PARUS.BLTBLROWS ro
        on(v.PRN = ro.RN)
        INNER JOIN PARUS.BLSUBREPORTS s
        on(ro.PRN = s.RN)
        INNER JOIN PARUS.BLREPORTS r
        on(s.PRN = r.RN)
        INNER JOIN PARUS.AGNLIST a
        on(r.AGENT = a.RN)
        INNER JOIN PARUS.BLREPFORMED rd
        on(r.BLREPFORMED = rd.RN)
        INNER JOIN PARUS.BLREPFORM rf
        on(rd.PRN = rf.RN)
        WHERE rf.code = '40 COVID 19'
        and r.BDATE =  trunc(SYSDATE-2)
        and i.CODE in ('Vaccin_TVSP','Vaccin_tvsp_03','Vaccin_tvsp_04','Vaccin_tvsp_04_day','Vaccin_tvsp_05',
                                        'Vaccin_tvsp_06','Vaccin_tvsp_07','Vaccin_tvsp_08', 'Vaccin_tvsp_09', 'Vaccin_tvsp_10',
                                        'Vaccin_tvsp_11', 'Vaccin_tvsp_12', 'Vaccin_tvsp_20','Vaccin_tvsp_20_day',
                                        'Vaccin_tvsp_13','Vaccin_tvsp_14','Vaccin_tvsp_15', 'Vaccin_tvsp_16', 'Vaccin_tvsp_17',
                                        'Vaccin_tvsp_18', 'Vaccin_tvsp_19', 'Vaccin_tvsp_19_day','Vaccin_tvsp_21', 'Vaccin_tvsp_21_day'
                                        , 'Vaccin_tvsp_23')
        )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_tvsp_03' Vaccin_tvsp_03,'Vaccin_tvsp_04' Vaccin_tvsp_04,'Vaccin_tvsp_04_day' Vaccin_tvsp_04_day,
                                        'Vaccin_tvsp_05' Vaccin_tvsp_05, 'Vaccin_tvsp_06' Vaccin_tvsp_06,'Vaccin_tvsp_07' Vaccin_tvsp_07,
                                        'Vaccin_tvsp_08' Vaccin_tvsp_08, 'Vaccin_tvsp_09' Vaccin_tvsp_09, 'Vaccin_tvsp_10' Vaccin_tvsp_10,
                                        'Vaccin_tvsp_11' Vaccin_tvsp_11, 'Vaccin_tvsp_12' Vaccin_tvsp_12, 'Vaccin_tvsp_20' Vaccin_tvsp_20,
                                        'Vaccin_tvsp_20_day' Vaccin_tvsp_20_day,'Vaccin_tvsp_13' Vaccin_tvsp_13,'Vaccin_tvsp_14' Vaccin_tvsp_14,
                                        'Vaccin_tvsp_15' Vaccin_tvsp_15, 'Vaccin_tvsp_16' Vaccin_tvsp_16, 'Vaccin_tvsp_17' Vaccin_tvsp_17,
                                        'Vaccin_tvsp_18' Vaccin_tvsp_18, 'Vaccin_tvsp_19' Vaccin_tvsp_19, 'Vaccin_tvsp_19_day' Vaccin_tvsp_19_day,
                                        'Vaccin_tvsp_21' Vaccin_tvsp_21, 'Vaccin_tvsp_21_day' Vaccin_tvsp_21_day, 'Vaccin_tvsp_23' Vaccin_tvsp_23)
        )
        ORDER BY ROW_INDEX"""

    try:
        #with sqlalchemy.create_engine("oracle+cx_oracle://" + base_parus + "/spb").connect() as connection:
        df = pd.read_sql(sql,con)
        old = pd.read_sql(sql2,con)
    except:
        raise my_except('Соединение с базой паруса дурит')
    
    df = df.loc[~df.tvsp.isin(['Пункт вакцинации'])]
    print(df.head(3))
    df.apply(pd.to_numeric,errors='ignore') #, downcast='integer')

    date_otch = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d_%m_%Y')
    new_name = date_otch + '_40_COVID_19_cvod.xlsx'
    shablon_path = get_dir('help')

    shutil.copyfile(shablon_path + '/40_COVID_19_svod.xlsx', shablon_path  + '/' + new_name)

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Пунты вакцинации']
    rows = dataframe_to_rows(df,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name) 


    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['Позавчера']
    rows = dataframe_to_rows(old,index=False, header=False)
    for r_idx, row in enumerate(rows,5):  
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)
    wb.save( shablon_path  + '/' + new_name) 
    return(shablon_path  + '/' + new_name)
