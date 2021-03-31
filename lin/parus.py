import sqlalchemy,cx_Oracle,os,openpyxl,shutil,datetime,subprocess
from openpyxl.utils.dataframe import dataframe_to_rows
import numpy as np
import pandas as pd
from loader import get_dir

base_parus = os.getenv('base_parus')

eng = sqlalchemy.create_engine("oracle+cx_oracle://" + base_parus + "/spb", pool_recycle=25)
#con = eng.connect()

class my_except(Exception):
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

    
    with eng.connect() as con:
        df = pd.read_sql(sql,con)
    
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

    with eng.connect() as con:
        df = pd.read_sql(sql,con)
        old = pd.read_sql(sql2,con)
   
    df = df.loc[~df.tvsp.isin(['Пункт вакцинации'])]
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

def parus_43_cov_nulls(a):
    sql="""
        SELECT 
                CAST(r.BDATE AS varchar(30))  day,
                a.AGNABBR code,
                a.AGNNAME  organization,
                sum(CASE WHEN NUMVAL = 0 THEN 1 ELSE 0 END ) nulls_in_itog
        FROM PARUS.BLINDEXVALUES  d
        INNER JOIN PARUS.BLSUBREPORTS s
        ON (d.PRN = s.RN)
        INNER JOIN PARUS.BLREPORTS r
        ON(s.PRN = r.RN)
        INNER JOIN PARUS.AGNLIST a 
        on(r.AGENT = a.rn)
        INNER JOIN PARUS.BLREPFORMED pf 
        on(r.BLREPFORMED = pf.RN)
        INNER JOIN PARUS.BLREPFORM rf 
        on(pf.PRN = rf.RN)
        INNER JOIN PARUS.BALANCEINDEXES bi 
        on(d.BALANCEINDEX = bi.RN)
        WHERE  rf.CODE = '43 COVID 19'
        and bi.CODE in ('43_covid_05','43_covid_07','43_covid_09_2')
        AND r.BDATE IN ( trunc(SYSDATE),  trunc(SYSDATE-1),  trunc(SYSDATE-2))
        GROUP BY r.BDATE,a.AGNABBR,a.AGNNAME
        HAVING sum(CASE WHEN NUMVAL = 0 THEN 1 ELSE 0 END ) > 1
            """
    
    with eng.connect() as con:
        df = pd.read_sql(sql,con)
    df = df.fillna(0)
    table_html = get_dir('temp') + '/table.html'
    table_png  = get_dir('temp') + '/table.png'

    df.to_html(table_html,justify='center', index=False)
    subprocess.call('wkhtmltoimage --quiet --encoding utf-8 -f png --width 0 ' +  table_html + ' ' + table_png, shell=True)
    return table_png

def svod_43_covid_19(a):
    sql = """
        SELECT  
                --DAY, organization,
                covid_02, covid_03,
                        cast(covid_04 AS int) covid_04 , cast(covid_05 AS int) covid_05,
                        cast(covid_06_old_2 AS int) covid_06_old_2 ,
                        cast(covid_06 AS int) covid_06 , cast(covid_07 AS int) covid_07,
                        cast(covid_08_old_2 AS int) covid_08_old_2, 
                        cast(covid_08 AS int) covid_08 , cast(covid_09 AS int) covid_09,
                        cast(covid_09_2 AS int) covid_09_2, cast(covid_10_old_2 AS int) covid_10_old_2,
                        cast(covid_10 AS int) covid_10 , cast(covid_11 AS int) covid_11
        FROM (
        SELECT 
                CAST(r.BDATE AS varchar(30))  day,
                a.AGNABBR  organization,
                bi.CODE pokazatel,
                CASE WHEN STRVAL  IS NOT NULL THEN STRVAL
                         WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
                         WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
                        ELSE NULL END value
        FROM PARUS.BLINDEXVALUES  d
        INNER JOIN PARUS.BLSUBREPORTS s
        ON (d.PRN = s.RN)
        INNER JOIN PARUS.BLREPORTS r
        ON(s.PRN = r.RN)
        INNER JOIN PARUS.AGNLIST a 
        on(r.AGENT = a.rn)
        INNER JOIN PARUS.BLREPFORMED pf 
        on(r.BLREPFORMED = pf.RN)
        INNER JOIN PARUS.BLREPFORM rf 
        on(pf.PRN = rf.RN)
        INNER JOIN PARUS.BALANCEINDEXES bi 
        on(d.BALANCEINDEX = bi.RN)
        WHERE  rf.CODE = '43 COVID 19'
        and bi.CODE in ('43_covid_02', '43_covid_03', '43_covid_04', '43_covid_05', '43_covid_06_old_2',
                                        '43_covid_06',  '43_covid_07', '43_covid_08_old_2', '43_covid_08', '43_covid_09', '43_covid_09_2',
                                        '43_covid_10_old_2', '43_covid_10', '43_covid_11')
        AND r.BDATE =  trunc(SYSDATE) )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('43_covid_02' covid_02, '43_covid_03' covid_03,
                                          '43_covid_04' covid_04, '43_covid_05' covid_05,
                                          '43_covid_06_old_2' covid_06_old_2,
                                          '43_covid_06' covid_06, '43_covid_07' covid_07,'43_covid_08_old_2' covid_08_old_2,
                                          '43_covid_08' covid_08, '43_covid_09' covid_09,
                                          '43_covid_09_2' covid_09_2, '43_covid_10_old_2' covid_10_old_2,
                                          '43_covid_10' covid_10, '43_covid_11' covid_11 )
        ) """

    if datetime.datetime.today().weekday() == 0:
        n = 3
    else:
        n = 1

    sql1 = f"""
        SELECT  
                --DAY, organization,
                covid_02, covid_03,
                        cast(covid_04 AS int) covid_04 , cast(covid_05 AS int) covid_05,
                        cast(covid_06_old_2 AS int) covid_06_old_2 ,
                        cast(covid_06 AS int) covid_06 , cast(covid_07 AS int) covid_07,
                        cast(covid_08_old_2 AS int) covid_08_old_2, 
                        cast(covid_08 AS int) covid_08 , cast(covid_09 AS int) covid_09,
                        cast(covid_09_2 AS int) covid_09_2, cast(covid_10_old_2 AS int) covid_10_old_2,
                        cast(covid_10 AS int) covid_10 , cast(covid_11 AS int) covid_11
        FROM (
        SELECT 
                CAST(r.BDATE AS varchar(30))  day,
                a.AGNABBR  organization,
                bi.CODE pokazatel,
                CASE WHEN STRVAL  IS NOT NULL THEN STRVAL
                         WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
                         WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
                        ELSE NULL END value
        FROM PARUS.BLINDEXVALUES  d
        INNER JOIN PARUS.BLSUBREPORTS s
        ON (d.PRN = s.RN)
        INNER JOIN PARUS.BLREPORTS r
        ON(s.PRN = r.RN)
        INNER JOIN PARUS.AGNLIST a 
        on(r.AGENT = a.rn)
        INNER JOIN PARUS.BLREPFORMED pf 
        on(r.BLREPFORMED = pf.RN)
        INNER JOIN PARUS.BLREPFORM rf 
        on(pf.PRN = rf.RN)
        INNER JOIN PARUS.BALANCEINDEXES bi 
        on(d.BALANCEINDEX = bi.RN)
        WHERE  rf.CODE = '43 COVID 19'
        and bi.CODE in ('43_covid_02', '43_covid_03', '43_covid_04', '43_covid_05', '43_covid_06_old_2',
                                        '43_covid_06',  '43_covid_07', '43_covid_08_old_2', '43_covid_08', '43_covid_09', '43_covid_09_2',
                                        '43_covid_10_old_2', '43_covid_10', '43_covid_11')
        AND r.BDATE =  trunc(SYSDATE - {n}) )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('43_covid_02' covid_02, '43_covid_03' covid_03,
                                          '43_covid_04' covid_04, '43_covid_05' covid_05,
                                          '43_covid_06_old_2' covid_06_old_2,
                                          '43_covid_06' covid_06, '43_covid_07' covid_07,'43_covid_08_old_2' covid_08_old_2,
                                          '43_covid_08' covid_08, '43_covid_09' covid_09,
                                          '43_covid_09_2' covid_09_2, '43_covid_10_old_2' covid_10_old_2,
                                          '43_covid_10' covid_10, '43_covid_11' covid_11 )
        ) """
    with eng.connect() as con:
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
    sql="""
         SELECT 
             a.AGNNAME "Наименование МО"
            ,a.AGNABBR "Мнемокод" 
            ,CASE 
              WHEN r.SENT =  1
                THEN 'Да'
                ELSE 'Нет'
             END "Отправлен на проверку"
          FROM PARUS.BLINDEXVALUES  d
            INNER JOIN PARUS.BLSUBREPORTS s
              ON (d.PRN = s.RN)
            INNER JOIN PARUS.BLREPORTS r
              ON (s.PRN = r.RN)
            INNER JOIN PARUS.AGNLIST a 
              ON (r.AGENT = a.rn)
            INNER JOIN PARUS.BLREPFORMED pf 
              ON (r.BLREPFORMED = pf.RN)
            INNER JOIN PARUS.BLREPFORM rf 
              ON (pf.PRN = rf.RN)
            INNER JOIN PARUS.BALANCEINDEXES bi 
              ON (d.BALANCEINDEX = bi.RN)
          WHERE rf.CODE = '43 COVID 19' -- наименование отчета
              AND r.BDATE = trunc(SYSDATE) -- отчет за сегодняшнюю дату
              AND bi.CODE = '43_covid_03' -- тип показателя, в котором указано наименование МО
              AND s.SAVE_RESULT = 0    -- кто не сохранил свой отчет    
          ORDER BY a.AGNABBR
                """
    with eng.connect() as con:
        df = pd.read_sql(sql,con)
    df = df.fillna(0)
    table_html = get_dir('temp') + '/table.html'
    table_png  = get_dir('temp') + '/table.png'

    df.to_html(table_html,justify='center', index=False)
    subprocess.call('wkhtmltoimage --quiet --encoding utf-8 -f png --width 0 ' +  table_html + ' ' + table_png, shell=True)
    return table_png