import sqlalchemy,cx_Oracle,os,openpyxl,shutil,datetime,subprocess,time,imgkit
from openpyxl.utils.dataframe import dataframe_to_rows
import numpy as np
import pandas as pd
from loader import get_dir

base_parus = os.getenv('base_parus')
userName = os.getenv('oracle_user')
password = os.getenv('oracle_pass')
userbase = os.getenv('oracle_base')

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
        and r.BDATE BETWEEN TO_DATE('30-01-2021','DD-MM-YYYY')  and trunc(sysdate)
        and i.CODE in ('Vaccin_TVSP','Vaccin_tvsp_18', 'Vaccin_tvsp_19_day' )
        )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_tvsp_18' V_1, 'Vaccin_tvsp_19_day' V_2 )
        )
        ORDER BY day, ROW_INDEX"""
    
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
    sql = """
    SELECT ORGANIZATION,'Пункт вакцинации' type, 
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
	    WHERE tvsp IS NOt NULL
    UNION
	    SELECT    			ORGANIZATION,	'Медицинская организация' type, 
			    substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,
			    case when tvsp is null then organization else tvsp end tvsp, 
			    CAST(Vaccin_03 AS int) Vaccin_03 , CAST(Vaccin_04 AS int) Vaccin_04, 
			    CAST(Vaccin_04_day AS int) Vaccin_04_day, CAST(Vaccin_05 AS int) Vaccin_05,
			    CAST(Vaccin_06 AS int) Vaccin_06, CAST(Vaccin_07 AS int) Vaccin_07, CAST(Vaccin_08 AS int) Vaccin_08,
			    CAST(Vaccin_09 AS int) Vaccin_09, CAST(Vaccin_10 AS int) Vaccin_10,
			    CAST(Vaccin_11 AS int) Vaccin_11, CAST(Vaccin_12 AS int) Vaccin_12, 
			    CAST(Vaccin_20 AS int) Vaccin_20, CAST(Vaccin_20_day AS int) Vaccin_20_day,
			    CAST(Vaccin_13 AS int) Vaccin_13, CAST(Vaccin_14 AS int) Vaccin_14, CAST(Vaccin_15 AS int) Vaccin_15,
			    CAST(Vaccin_16 AS int) Vaccin_16, CAST(Vaccin_17 AS int) Vaccin_17,
			    CAST(Vaccin_18 AS int) Vaccin_18, CAST(Vaccin_19 AS int) Vaccin_19, CAST(Vaccin_19_day AS int) Vaccin_19_day,
			    CAST(Vaccin_21 AS int) Vaccin_21, CAST(Vaccin_21_day AS int) Vaccin_21_day, CAST(Vaccin_23 AS int) Vaccin_23 
	    FROM (
	    SELECT
		    r.BDATE,
		    a.AGNNAME ORGANIZATION ,
		    rf.CODE  otchet,
		    bi.CODE  pokazatel,
	    CASE WHEN STRVAL IS NOT NULL THEN STRVAL 
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
	    WHERE rf.code = '40 COVID 19'
	     and  r.BDATE =  trunc(SYSDATE-1)
	    and bi.CODE in ('Vaccin_TVSP','Vaccin_03','Vaccin_04','Vaccin_04_day','Vaccin_05',
					    'Vaccin_06','Vaccin_07','Vaccin_08', 'Vaccin_09', 'Vaccin_10',
					    'Vaccin_11', 'Vaccin_12', 'Vaccin_20','Vaccin_20_day',
					    'Vaccin_13','Vaccin_14','Vaccin_15', 'Vaccin_16', 'Vaccin_17',
					    'Vaccin_18', 'Vaccin_19', 'Vaccin_19_day','Vaccin_21', 'Vaccin_21_day'
					    , 'Vaccin_23')
	    )
	    pivot
	    (
	    max(value)
	    FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_03' Vaccin_03,'Vaccin_04' Vaccin_04,'Vaccin_04_day' Vaccin_04_day,
					    'Vaccin_05' Vaccin_05, 'Vaccin_06' Vaccin_06,'Vaccin_07' Vaccin_07,
					    'Vaccin_08' Vaccin_08, 'Vaccin_09' Vaccin_09, 'Vaccin_10' Vaccin_10,
					    'Vaccin_11' Vaccin_11, 'Vaccin_12' Vaccin_12, 'Vaccin_20' Vaccin_20,
					    'Vaccin_20_day' Vaccin_20_day,'Vaccin_13' Vaccin_13,'Vaccin_14' Vaccin_14,
					    'Vaccin_15' Vaccin_15, 'Vaccin_16' Vaccin_16, 'Vaccin_17' Vaccin_17,
					    'Vaccin_18' Vaccin_18, 'Vaccin_19' Vaccin_19, 'Vaccin_19_day' Vaccin_19_day,
					    'Vaccin_21' Vaccin_21, 'Vaccin_21_day' Vaccin_21_day, 'Vaccin_23' Vaccin_23)
	    )
    ORDER BY ORGANIZATION   """
    
    sql2 = """
    SELECT    				ORGANIZATION,
                                                    'Пункт вакцинации' type, 
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
            WHERE tvsp IS NOt NULL
    UNION
            SELECT    			ORGANIZATION,	'Медицинская организация' type, 
                            substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,
                            case when tvsp is null then organization else tvsp end tvsp, 
                            CAST(Vaccin_03 AS int) Vaccin_03 , CAST(Vaccin_04 AS int) Vaccin_04, 
                            CAST(Vaccin_04_day AS int) Vaccin_04_day, CAST(Vaccin_05 AS int) Vaccin_05,
                            CAST(Vaccin_06 AS int) Vaccin_06, CAST(Vaccin_07 AS int) Vaccin_07, CAST(Vaccin_08 AS int) Vaccin_08,
                            CAST(Vaccin_09 AS int) Vaccin_09, CAST(Vaccin_10 AS int) Vaccin_10,
                            CAST(Vaccin_11 AS int) Vaccin_11, CAST(Vaccin_12 AS int) Vaccin_12, 
                            CAST(Vaccin_20 AS int) Vaccin_20, CAST(Vaccin_20_day AS int) Vaccin_20_day,
                            CAST(Vaccin_13 AS int) Vaccin_13, CAST(Vaccin_14 AS int) Vaccin_14, CAST(Vaccin_15 AS int) Vaccin_15,
                            CAST(Vaccin_16 AS int) Vaccin_16, CAST(Vaccin_17 AS int) Vaccin_17,
                            CAST(Vaccin_18 AS int) Vaccin_18, CAST(Vaccin_19 AS int) Vaccin_19, CAST(Vaccin_19_day AS int) Vaccin_19_day,
                            CAST(Vaccin_21 AS int) Vaccin_21, CAST(Vaccin_21_day AS int) Vaccin_21_day, CAST(Vaccin_23 AS int) Vaccin_23 
            FROM (
            SELECT
                    r.BDATE,
                    a.AGNNAME ORGANIZATION ,
                    rf.CODE  otchet,
                    bi.CODE  pokazatel,
            CASE WHEN STRVAL IS NOT NULL THEN STRVAL 
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
            WHERE rf.code = '40 COVID 19'
             and  r.BDATE =  trunc(SYSDATE-2)
            and bi.CODE in ('Vaccin_TVSP','Vaccin_03','Vaccin_04','Vaccin_04_day','Vaccin_05',
                                            'Vaccin_06','Vaccin_07','Vaccin_08', 'Vaccin_09', 'Vaccin_10',
                                            'Vaccin_11', 'Vaccin_12', 'Vaccin_20','Vaccin_20_day',
                                            'Vaccin_13','Vaccin_14','Vaccin_15', 'Vaccin_16', 'Vaccin_17',
                                            'Vaccin_18', 'Vaccin_19', 'Vaccin_19_day','Vaccin_21', 'Vaccin_21_day'
                                            , 'Vaccin_23')
            )
            pivot
            (
            max(value)
            FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_03' Vaccin_03,'Vaccin_04' Vaccin_04,'Vaccin_04_day' Vaccin_04_day,
                                            'Vaccin_05' Vaccin_05, 'Vaccin_06' Vaccin_06,'Vaccin_07' Vaccin_07,
                                            'Vaccin_08' Vaccin_08, 'Vaccin_09' Vaccin_09, 'Vaccin_10' Vaccin_10,
                                            'Vaccin_11' Vaccin_11, 'Vaccin_12' Vaccin_12, 'Vaccin_20' Vaccin_20,
                                            'Vaccin_20_day' Vaccin_20_day,'Vaccin_13' Vaccin_13,'Vaccin_14' Vaccin_14,
                                            'Vaccin_15' Vaccin_15, 'Vaccin_16' Vaccin_16, 'Vaccin_17' Vaccin_17,
                                            'Vaccin_18' Vaccin_18, 'Vaccin_19' Vaccin_19, 'Vaccin_19_day' Vaccin_19_day,
                                            'Vaccin_21' Vaccin_21, 'Vaccin_21_day' Vaccin_21_day, 'Vaccin_23' Vaccin_23)
            )
    ORDER BY ORGANIZATION,TYPE 
    """

    sql3 = """
    SELECT ORGANIZATION, 'Пункт вакцинации' type,  substr(EVC_TVSP_2_02 ,1,INSTR(EVC_TVSP_2_02 , ' ')-1) dist,EVC_TVSP_2_02,
    nvl(cast(EVC_TVSP_2_04 as int),0)  EVC_TVSP_2_04,
    nvl(cast(EVC_TVSP_2_05_z as int),0)  EVC_TVSP_2_05_z,nvl(cast(EVC_TVSP_2_06 as int),0)  EVC_TVSP_2_06,
    nvl(cast(EVC_TVSP_2_07_z as int),0)  EVC_TVSP_2_07_z,
    nvl(cast(EVC_TVSP_2_08 as int),0)  EVC_TVSP_2_08,
    nvl(cast(EVC_TVSP_2_09_z as int),0)  EVC_TVSP_2_09_z,nvl(cast(EVC_TVSP_2_10 as int),0)  EVC_TVSP_2_10,
    nvl(cast(EVC_TVSP_2_11_z as int),0)  EVC_TVSP_2_11_z,
    nvl(cast(EVC_TVSP_2_12 as int),0)  EVC_TVSP_2_12,
    nvl(cast(EVC_TVSP_2_13_z as int),0)  EVC_TVSP_2_13_z,nvl(cast(EVC_TVSP_2_14 as int),0)  EVC_TVSP_2_14,
    nvl(cast(EVC_TVSP_2_15_z as int),0)  EVC_TVSP_2_15_z,
    nvl(cast(EVC_TVSP_2_16 as int),0)  EVC_TVSP_2_16,
    nvl(cast(EVC_TVSP_2_17_z as int),0)  EVC_TVSP_2_17_z,nvl(cast(EVC_TVSP_2_18 as int),0)  EVC_TVSP_2_18,
    nvl(cast(EVC_TVSP_2_19_z as int),0)  EVC_TVSP_2_19_z,
    nvl(cast(EVC_TVSP_2_20 as int),0)  EVC_TVSP_2_20,nvl(cast(EVC_TVSP_2_21 as int),0)  EVC_TVSP_2_21,
    nvl(cast(EVC_TVSP_2_22 as int),0)  EVC_TVSP_2_22,
    nvl(cast(EVC_TVSP_2_23_z as int),0)  EVC_TVSP_2_23_z,nvl(cast(EVC_TVSP_2_24 as int),0)  EVC_TVSP_2_24
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
             and i.CODE in ('EVC_TVSP_2_02','EVC_TVSP_2_04','EVC_TVSP_2_05_z',
                                            'EVC_TVSP_2_06','EVC_TVSP_2_07_z', 'EVC_TVSP_2_08', 
                                            'EVC_TVSP_2_09_z', 'EVC_TVSP_2_10', 'EVC_TVSP_2_11_z',
                                            'EVC_TVSP_2_12','EVC_TVSP_2_13_z', 'EVC_TVSP_2_14', 
                                            'EVC_TVSP_2_15_z', 'EVC_TVSP_2_16','EVC_TVSP_2_17_z', 'EVC_TVSP_2_18'
                                            , 'EVC_TVSP_2_19_z', 'EVC_TVSP_2_20', 'EVC_TVSP_2_21', 'EVC_TVSP_2_22'
                                            , 'EVC_TVSP_2_23_z', 'EVC_TVSP_2_24')
            )
            pivot
            (
            max(value)
            FOR POKAZATEL IN ('EVC_TVSP_2_02'  EVC_TVSP_2_02,
    'EVC_TVSP_2_04'  EVC_TVSP_2_04,
    'EVC_TVSP_2_05_z'  EVC_TVSP_2_05_z,'EVC_TVSP_2_06'  EVC_TVSP_2_06,
    'EVC_TVSP_2_07_z'  EVC_TVSP_2_07_z,
    'EVC_TVSP_2_08'  EVC_TVSP_2_08,
    'EVC_TVSP_2_09_z'  EVC_TVSP_2_09_z,'EVC_TVSP_2_10'  EVC_TVSP_2_10,
    'EVC_TVSP_2_11_z'  EVC_TVSP_2_11_z,
    'EVC_TVSP_2_12'  EVC_TVSP_2_12,
    'EVC_TVSP_2_13_z'  EVC_TVSP_2_13_z,'EVC_TVSP_2_14'  EVC_TVSP_2_14,
    'EVC_TVSP_2_15_z'  EVC_TVSP_2_15_z,
    'EVC_TVSP_2_16'  EVC_TVSP_2_16,
    'EVC_TVSP_2_17_z'  EVC_TVSP_2_17_z,'EVC_TVSP_2_18'  EVC_TVSP_2_18,
    'EVC_TVSP_2_19_z'  EVC_TVSP_2_19_z,
    'EVC_TVSP_2_20'  EVC_TVSP_2_20,'EVC_TVSP_2_21'  EVC_TVSP_2_21,
    'EVC_TVSP_2_22'  EVC_TVSP_2_22, 'EVC_TVSP_2_23_z'  EVC_TVSP_2_23_z,
    'EVC_TVSP_2_24'  EVC_TVSP_2_24
    )
            )
    UNION
    SELECT ORGANIZATION, 'Медицинская организация' TYPE, EVC_TVSP_01 dist, case when EVC_TVSP_02 is null then ORGANIZATION else  EVC_TVSP_02 END EVC_TVSP_02,
    nvl(cast(EVC_TVSP_04 as int),0)  EVC_TVSP_04,nvl(cast(EVC_TVSP_05_z as int),0)  EVC_TVSP_05_z,
    nvl(cast(EVC_TVSP_06 as int),0)  EVC_TVSP_06,nvl(cast(EVC_TVSP_07_z as int),0)  EVC_TVSP_07_z,
    nvl(cast(EVC_TVSP_08 as int),0)  EVC_TVSP_08,nvl(cast(EVC_TVSP_09_z as int),0)  EVC_TVSP_09_z,
    nvl(cast(EVC_TVSP_10 as int),0)  EVC_TVSP_10,nvl(cast(EVC_TVSP_11_z as int),0)  EVC_TVSP_11_z,
    nvl(cast(EVC_TVSP_12 as int),0)  EVC_TVSP_12,nvl(cast(EVC_TVSP_13_z as int),0)  EVC_TVSP_13_z,
    nvl(cast(EVC_TVSP_14 as int),0)  EVC_TVSP_14,nvl(cast(EVC_TVSP_15_z as int),0)  EVC_TVSP_15_z,
    nvl(cast(EVC_TVSP_16 as int),0)  EVC_TVSP_16,nvl(cast(EVC_TVSP_17_z as int),0)  EVC_TVSP_17_z,
    nvl(cast(EVC_TVSP_18 as int),0)  EVC_TVSP_18,nvl(cast(EVC_TVSP_19_z as int),0)  EVC_TVSP_19_z,
    nvl(cast(EVC_TVSP_20 as int),0)  EVC_TVSP_20,nvl(cast(EVC_TVSP_21   as int),0)  EVC_TVSP_21,
    nvl(cast(EVC_TVSP_22 as int),0)  EVC_TVSP_22,nvl(cast(EVC_TVSP_23_z as int),0)  EVC_TVSP_23_z,
    nvl(cast(EVC_TVSP_24 as int),0)  EVC_TVSP_24
            FROM (
            SELECT
            to_char(r.BDATE, 'DD.MM.YYYY') day,
                    a.AGNNAME ORGANIZATION ,
                    rf.CODE  otchet,
                    bi.CODE  pokazatel,
            CASE WHEN STRVAL IS NOT NULL THEN STRVAL 
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
            WHERE rf.code = '40 COVID 19'
             and  r.BDATE =  trunc(SYSDATE-1)
             and bi.CODE in ('EVC_TVSP_01','EVC_TVSP_02','EVC_TVSP_04','EVC_TVSP_05_z',
                                            'EVC_TVSP_06','EVC_TVSP_07_z', 'EVC_TVSP_08', 
                                            'EVC_TVSP_09_z', 'EVC_TVSP_10', 'EVC_TVSP_11_z',
                                            'EVC_TVSP_12','EVC_TVSP_13_z', 'EVC_TVSP_14', 
                                            'EVC_TVSP_15_z', 'EVC_TVSP_16','EVC_TVSP_17_z', 'EVC_TVSP_18'
                                            , 'EVC_TVSP_19_z', 'EVC_TVSP_20', 'EVC_TVSP_21', 'EVC_TVSP_22'
                                            , 'EVC_TVSP_23_z', 'EVC_TVSP_24')
            )
            pivot
            (
            max(value)
            FOR POKAZATEL IN ('EVC_TVSP_01'  EVC_TVSP_01,'EVC_TVSP_02'  EVC_TVSP_02,
    'EVC_TVSP_04'  EVC_TVSP_04,'EVC_TVSP_05_z'  EVC_TVSP_05_z,'EVC_TVSP_06'  EVC_TVSP_06,
    'EVC_TVSP_07_z'  EVC_TVSP_07_z,'EVC_TVSP_08'  EVC_TVSP_08,'EVC_TVSP_09_z'  EVC_TVSP_09_z,
    'EVC_TVSP_10'  EVC_TVSP_10,'EVC_TVSP_11_z'  EVC_TVSP_11_z,'EVC_TVSP_12'  EVC_TVSP_12,
    'EVC_TVSP_13_z'  EVC_TVSP_13_z,'EVC_TVSP_14'  EVC_TVSP_14,'EVC_TVSP_15_z'  EVC_TVSP_15_z,
    'EVC_TVSP_16'  EVC_TVSP_16,'EVC_TVSP_17_z'  EVC_TVSP_17_z,'EVC_TVSP_18'  EVC_TVSP_18,
    'EVC_TVSP_19_z'  EVC_TVSP_19_z,'EVC_TVSP_20'  EVC_TVSP_20,'EVC_TVSP_21'  EVC_TVSP_21,
    'EVC_TVSP_22'  EVC_TVSP_22,'EVC_TVSP_23_z'  EVC_TVSP_23_z,'EVC_TVSP_24'  EVC_TVSP_24
    )
            )
    """
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        df = pd.read_sql(sql,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        old = pd.read_sql(sql2,con)
    with cx_Oracle.connect(userName, password, userbase,encoding="UTF-8") as con:
        epivak = pd.read_sql(sql3,con)
  
    df = df.loc[~df.TVSP.isin(['Пункт вакцинации'])]
    df.apply(pd.to_numeric,errors='ignore') #, downcast='integer')

    epivak.sort_values(by=['ORGANIZATION','TYPE'])
    del df ['ORGANIZATION']
    del old ['ORGANIZATION']
    del epivak ['ORGANIZATION']

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

    wb= openpyxl.load_workbook( shablon_path  + '/' + new_name)
    ws = wb['ЭпиВакКорона']
    rows = dataframe_to_rows(epivak,index=False, header=False)
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

def cvod_29_covid(a): 
    if int(time.strftime("%H")) < 16:
        n = 1
        date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d_%m_%Y')
    else:
        n = 0
        date = datetime.datetime.now().strftime('%d_%m_%Y')
    sql = f"""
    SELECT  DAY, pok_01, nvl(cast(pok_02 as int),0) pok_02,
    nvl(cast(pok_03 as int),0) pok_03,nvl(cast(pok_04 as int),0) pok_04,nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,
    nvl(cast(pok_07 as int),0) pok_07,nvl(cast(pok_08 as int),0) pok_08,nvl(cast(pok_09 as int),0) pok_09,nvl(cast(pok_10 as int),0) pok_10,
    nvl(cast(pok_11 as int),0) pok_11,nvl(cast(pok_12 as int),0) pok_12,nvl(cast(pok_13 as int),0) pok_13,nvl(cast(pok_14 as int),0) pok_14,
    nvl(cast(pok_15 as int),0) pok_15,nvl(cast(pok_16 as int),0) pok_16,nvl(cast(pok_17 as int),0) pok_17,nvl(cast(pok_18 as int),0) pok_18,
    nvl(cast(pok_19 as int),0) pok_19,nvl(cast(pok_20 as int),0) pok_20,nvl(cast(pok_21 as int),0) pok_21,nvl(cast(pok_22 as int),0) pok_22,
    nvl(cast(pok_23 as int),0) pok_23,nvl(cast(pok_24 as int),0) pok_24,nvl(cast(pok_25 as int),0) pok_25,nvl(cast(pok_26 as int),0) pok_26,
    nvl(cast(pok_27 as int),0) pok_27,nvl(cast(pok_28 as int),0) pok_28,nvl(cast(pok_29 as int),0) pok_29,nvl(cast(pok_30 as int),0) pok_30,
    nvl(cast(pok_31 as int),0) pok_31,nvl(cast(pok_32 as int),0) pok_32,nvl(cast(pok_33 as int),0) pok_33,nvl(cast(pok_34 as int),0) pok_34,
    nvl(cast(pok_35 as int),0) pok_35,nvl(cast(pok_36 as int),0) pok_36,nvl(cast(pok_37 as int),0) pok_37,nvl(cast(pok_38 as int),0) pok_38,
    nvl(cast(pok_39 as int),0) pok_39,nvl(cast(pok_40 as int),0) pok_40,nvl(cast(pok_41 as int),0) pok_41,nvl(cast(pok_42 as int),0) pok_42,
    nvl(cast(pok_43 as int),0) pok_43,
    nvl(cast(pok_44 as float),0) pok_44,nvl(cast(pok_45 as float),0) pok_45,nvl(cast(pok_46 as float),0) pok_46,
    nvl(cast(pok_47 as float),0) pok_47,nvl(cast(pok_48 as float),0) pok_48,nvl(cast(pok_49 as float),0) pok_49,nvl(cast(pok_50 as float),0) pok_50,
    nvl(cast(pok_51 as float),0) pok_51,nvl(cast(pok_52 as float),0) pok_52,nvl(cast(pok_53 as float),0) pok_53,
    nvl(cast(pok_54 as int),0) pok_54,nvl(cast(pok_55 as float),0) pok_55
    FROM (
    SELECT 
            to_char(r.BDATE, 'DD.MM.YYYY')  day,
            a.AGNNAME ORGANIZATION ,
            rf.CODE  otchet,
            bi.CODE  pokazatel,
        CASE WHEN STRVAL IS NOT NULL THEN STRVAL 
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
    WHERE rf.CODE = '29 COVID 19' 
    and r.BDATE =  trunc(SYSDATE-{n})
    and bi.CODE LIKE '29_covid_0%'
    order by  d.BALANCEINDEX 
    )
    pivot
    (
    MIN(value)
    FOR POKAZATEL IN ('29_covid_001' pok_01,'29_covid_002' pok_02,'29_covid_003' pok_03,'29_covid_004' pok_04,
    '29_covid_005' pok_05,'29_covid_006' pok_06,'29_covid_007' pok_07,'29_covid_008' pok_08,
    '29_covid_009' pok_09,'29_covid_010' pok_10,'29_covid_011' pok_11,'29_covid_012' pok_12,
    '29_covid_013' pok_13,'29_covid_014' pok_14,'29_covid_015' pok_15,'29_covid_016' pok_16,
    '29_covid_017' pok_17,'29_covid_018' pok_18,'29_covid_019' pok_19,'29_covid_020' pok_20,
    '29_covid_021' pok_21,'29_covid_022' pok_22,'29_covid_023' pok_23,'29_covid_024' pok_24,
    '29_covid_025' pok_25,'29_covid_026' pok_26,'29_covid_027' pok_27,'29_covid_028' pok_28,
    '29_covid_029' pok_29,'29_covid_030' pok_30,'29_covid_031' pok_31,'29_covid_032' pok_32,
    '29_covid_033' pok_33,'29_covid_034' pok_34,'29_covid_035' pok_35,'29_covid_036' pok_36,
    '29_covid_037' pok_37,'29_covid_038' pok_38,'29_covid_039' pok_39,'29_covid_040' pok_40,
    '29_covid_041' pok_41,'29_covid_042' pok_42,'29_covid_043' pok_43,'29_covid_044' pok_44,
    '29_covid_045' pok_45,'29_covid_046' pok_46,'29_covid_047' pok_47,'29_covid_048' pok_48,
    '29_covid_049' pok_49,'29_covid_050' pok_50,'29_covid_051' pok_51,'29_covid_052' pok_52,
    '29_covid_053' pok_53,'29_covid_054' pok_54,'29_covid_055' pok_55)
    )
    WHERE POK_01 IS NOT NULL 
    ORDER BY POK_01
    """
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
    sql = """
    SELECT  Trp_17_MO, Trp_18_dist,
            nvl(cast(Trp_01 as int),0) Trp_01, nvl(cast(Trp_02 as int),0) Trp_02,
            nvl(cast(Trp_03 as int),0) Trp_03, nvl(cast(Trp_04 as int),0) Trp_04,
            nvl(cast(Trp_05 as int),0) Trp_05, nvl(cast(Trp_06 as int),0) Trp_06,
            nvl(cast(Trp_07 as int),0) Trp_07, nvl(cast(Trp_07 as int),0) Trp_07,
            nvl(cast(Trp_08 as int),0) Trp_08, nvl(cast(Trp_09 as int),0) Trp_09,
            nvl(cast(Trp_10 as int),0) Trp_10, nvl(cast(Trp_11 as int),0) Trp_11,
            nvl(cast(Trp_12 as int),0) Trp_12, nvl(cast(Trp_13 as int),0) Trp_13,
            nvl(cast(Trp_14 as int),0) Trp_14, nvl(cast(Trp_15 as int),0) Trp_15,
            nvl(cast(Trp_16 as int),0) Trp_16
    FROM (
    SELECT 
            to_char(r.BDATE, 'DD.MM.YYYY') day,
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
    WHERE rf.code = '33 COVID 19'
    and r.BDATE = (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
				INNER JOIN PARUS.BLTBLROWS ro 
				on(v.PRN = ro.RN)
				INNER JOIN PARUS.BLSUBREPORTS s 
				on(ro.PRN = s.RN)
				INNER JOIN PARUS.BLREPORTS r
				on(s.PRN = r.RN)
				INNER JOIN PARUS.BLREPFORMED rd
				on(r.BLREPFORMED = rd.RN)
				INNER JOIN PARUS.BLREPFORM rf
				on(rd.PRN = rf.RN)
				WHERE rf.code = '33 COVID 19') 
    AND i.CODE IN ('Trp_17_MO', 'Trp_18_dist', 'Trp_01', 'Trp_02', 'Trp_03', 'Trp_04', 'Trp_05', 'Trp_06',
            'Trp_07', 'Trp_08','Trp_09', 'Trp_10', 'Trp_11', 'Trp_12', 'Trp_13', 'Trp_14', 'Trp_15', 'Trp_16' )
    )
    pivot
    (
    MIN(value)
    FOR POKAZATEL IN (  'Trp_17_MO' Trp_17_MO, 'Trp_18_dist' Trp_18_dist, 'Trp_01' Trp_01, 'Trp_02' Trp_02,
                                            'Trp_03' Trp_03, 'Trp_04' Trp_04, 'Trp_05' Trp_05 , 'Trp_06' Trp_06,
                                    'Trp_07' Trp_07, 'Trp_08' Trp_08, 'Trp_09' Trp_09, 'Trp_10' Trp_10, 'Trp_11' Trp_11,
                                    'Trp_12' Trp_12, 'Trp_13' Trp_13, 'Trp_14' Trp_14, 'Trp_15' Trp_15, 'Trp_16' Trp_16 )    			
    )
    WHERE Trp_17_MO IS NOT NULL 
    ORDER BY Trp_18_dist
    """

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
    sql = """
    SELECT distinct DAY, Distr_doc_19_MO, Distr_doc_20_Dist,
            nvl(cast(Distr_doc_01 as int),0) Distr_doc_01, nvl(cast(Distr_doc_02 as int),0) Distr_doc_02,
            nvl(cast(Distr_doc_03 as int),0) Distr_doc_03, nvl(cast(Distr_doc_04 as int),0) Distr_doc_04,
            nvl(cast(Distr_doc_05 as int),0) Distr_doc_05, nvl(cast(Distr_doc_06 as int),0) Distr_doc_06,
            nvl(cast(Distr_doc_07 as int),0) Distr_doc_07, nvl(cast(Distr_doc_08 as int),0) Distr_doc_08,
            nvl(cast(Distr_doc_09 as int),0) Distr_doc_09, nvl(cast(Distr_doc_10 as int),0) Distr_doc_10,
            nvl(cast(Distr_doc_11 as int),0) Distr_doc_11, nvl(cast(Distr_doc_12 as int),0) Distr_doc_12,
            nvl(cast(Distr_doc_13 as int),0) Distr_doc_13, nvl(cast(Distr_doc_14 as int),0) Distr_doc_14,
            nvl(cast(Distr_doc_15 as int),0) Distr_doc_15, nvl(cast(Distr_doc_16 as int),0) Distr_doc_16,
            nvl(cast(Distr_doc_17 as int),0) Distr_doc_17, nvl(cast(Distr_doc_18 as int),0) Distr_doc_18
    FROM (
    SELECT 
            to_char(r.BDATE, 'DD.MM.YYYY') day,
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
    WHERE rf.code = '36 COVID 19'
    AND i.CODE IN ('Distr_doc_19_MO', 'Distr_doc_20_Dist', 'Distr_doc_01', 'Distr_doc_02', 'Distr_doc_03',
                                    'Distr_doc_04', 'Distr_doc_05', 'Distr_doc_06','Distr_doc_07', 'Distr_doc_08','Distr_doc_09',
                                    'Distr_doc_10', 'Distr_doc_11', 'Distr_doc_12', 'Distr_doc_13', 'Distr_doc_14', 
                                    'Distr_doc_15', 'Distr_doc_16', 'Distr_doc_17', 'Distr_doc_18' )
    and r.BDATE = (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
                                    INNER JOIN PARUS.BLTBLROWS ro 
                                    on(v.PRN = ro.RN)
                                    INNER JOIN PARUS.BLSUBREPORTS s 
                                    on(ro.PRN = s.RN)
                                    INNER JOIN PARUS.BLREPORTS r
                                    on(s.PRN = r.RN)
                                    INNER JOIN PARUS.BLREPFORMED rd
                                    on(r.BLREPFORMED = rd.RN)
                                    INNER JOIN PARUS.BLREPFORM rf
                                    on(rd.PRN = rf.RN)
                                    WHERE rf.code = '36 COVID 19')
    )
    pivot
    (
    MIN(value)
    FOR POKAZATEL IN (  'Distr_doc_19_MO' Distr_doc_19_MO, 'Distr_doc_20_Dist' Distr_doc_20_Dist, 'Distr_doc_01' Distr_doc_01,
                                            'Distr_doc_02' Distr_doc_02, 'Distr_doc_03' Distr_doc_03, 'Distr_doc_04' Distr_doc_04, 'Distr_doc_05' Distr_doc_05,
                                            'Distr_doc_06' Distr_doc_06, 'Distr_doc_07' Distr_doc_07, 'Distr_doc_08' Distr_doc_08, 'Distr_doc_09' Distr_doc_09,
                                            'Distr_doc_10' Distr_doc_10, 'Distr_doc_11' Distr_doc_11, 'Distr_doc_12' Distr_doc_12, 'Distr_doc_13' Distr_doc_13,
                                            'Distr_doc_14' Distr_doc_14, 'Distr_doc_15' Distr_doc_15, 'Distr_doc_16' Distr_doc_16, 'Distr_doc_17' Distr_doc_17,
                                            'Distr_doc_18' Distr_doc_18)    			
    )
    WHERE Distr_doc_19_MO IS NOT NULL 
    ORDER BY Distr_doc_20_Dist
    """

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
    sql = """
    SELECT distinct DAY, Polic_ats_MO, Polic_ats_dist,
            nvl(cast(Polic_ats_01 as int),0) Polic_ats_01, nvl(cast(Polic_ats_02 as int),0) Polic_ats_02,
            nvl(cast(Polic_ats_03 as int),0) Polic_ats_03, nvl(cast(Polic_ats_04 as int),0) Polic_ats_04,
            nvl(cast(Polic_ats_05 as int),0) Polic_ats_05, nvl(cast(Polic_ats_06 as int),0) Polic_ats_06,
            Polic_ats_07, Polic_ats_08, Polic_ats_09, Polic_ats_10, Polic_ats_11, Polic_ats_12, Polic_ats_13,
            nvl(cast(Polic_ats_14 as int),0) Polic_ats_14,nvl(cast(Polic_ats_15 as int),0) Polic_ats_15,
            nvl(cast(Polic_ats_16 as int),0) Polic_ats_16,
            nvl(cast(Polic_ats_17 as float),0) Polic_ats_17, nvl(cast(Polic_ats_18 as float),0) Polic_ats_18
    FROM (
    SELECT 
            to_char(r.BDATE, 'DD.MM.YYYY') day,
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
    WHERE rf.code = '37 COVID 19'
    AND i.CODE IN ('Polic_ats_MO', 'Polic_ats_dist', 'Polic_ats_01', 'Polic_ats_02', 'Polic_ats_03',
                                    'Polic_ats_04', 'Polic_ats_05', 'Polic_ats_06','Polic_ats_07', 'Polic_ats_08','Polic_ats_09',
                                    'Polic_ats_10', 'Polic_ats_11', 'Polic_ats_12', 'Polic_ats_13', 'Polic_ats_14', 
                                    'Polic_ats_15', 'Polic_ats_16', 'Polic_ats_17', 'Polic_ats_18' )
    and r.BDATE = (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
                                    INNER JOIN PARUS.BLTBLROWS ro 
                                    on(v.PRN = ro.RN)
                                    INNER JOIN PARUS.BLSUBREPORTS s 
                                    on(ro.PRN = s.RN)
                                    INNER JOIN PARUS.BLREPORTS r
                                    on(s.PRN = r.RN)
                                    INNER JOIN PARUS.BLREPFORMED rd
                                    on(r.BLREPFORMED = rd.RN)
                                    INNER JOIN PARUS.BLREPFORM rf
                                    on(rd.PRN = rf.RN)
                                    WHERE rf.code = '37 COVID 19')
    )
    pivot
    (
    MIN(value)
    FOR POKAZATEL IN (  'Polic_ats_MO' Polic_ats_MO, 'Polic_ats_dist' Polic_ats_dist, 'Polic_ats_01' Polic_ats_01,
                                            'Polic_ats_02' Polic_ats_02, 'Polic_ats_03' Polic_ats_03, 'Polic_ats_04' Polic_ats_04, 'Polic_ats_05' Polic_ats_05,
                                            'Polic_ats_06' Polic_ats_06, 'Polic_ats_07' Polic_ats_07, 'Polic_ats_08' Polic_ats_08, 'Polic_ats_09' Polic_ats_09,
                                            'Polic_ats_10' Polic_ats_10, 'Polic_ats_11' Polic_ats_11, 'Polic_ats_12' Polic_ats_12, 'Polic_ats_13' Polic_ats_13,
                                            'Polic_ats_14' Polic_ats_14, 'Polic_ats_15' Polic_ats_15, 'Polic_ats_16' Polic_ats_16, 'Polic_ats_17' Polic_ats_17,
                                            'Polic_ats_18' Polic_ats_18)    			
    )
    WHERE Polic_ats_MO IS NOT NULL 
    ORDER BY Polic_ats_dist
    """

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
    sql = """
    SELECT distinct DAY, Step_risk_15_MO, Step_risk_16_dist,
            nvl(cast(Step_risk_01 as int),0) Step_risk_01, nvl(cast(Step_risk_02 as int),0) Step_risk_02,
            nvl(cast(Step_risk_03 as int),0) Step_risk_03, nvl(cast(Step_risk_04 as int),0) Step_risk_04,
            nvl(cast(Step_risk_05 as int),0) Step_risk_05, nvl(cast(Step_risk_06 as int),0) Step_risk_06,
            nvl(cast(Step_risk_07 as int),0) Step_risk_07, nvl(cast(Step_risk_08 as int),0) Step_risk_08,
            nvl(cast(Step_risk_09 as int),0) Step_risk_09, nvl(cast(Step_risk_10 as int),0) Step_risk_10,
            nvl(cast(Step_risk_11 as int),0) Step_risk_11, nvl(cast(Step_risk_12 as int),0) Step_risk_12,
            nvl(cast(Step_risk_13 as int),0) Step_risk_13, nvl(cast(Step_risk_14 as int),0) Step_risk_14
    FROM (
    SELECT 
            to_char(r.BDATE, 'DD.MM.YYYY') day,
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
    WHERE rf.code = '38 COVID 19'
    AND i.CODE IN ('Step_risk_15_MO', 'Step_risk_16_dist', 'Step_risk_01', 'Step_risk_02', 'Step_risk_03',
                                    'Step_risk_04', 'Step_risk_05', 'Step_risk_06','Step_risk_07', 'Step_risk_08','Step_risk_09',
                                    'Step_risk_10', 'Step_risk_11', 'Step_risk_12', 'Step_risk_13', 'Step_risk_14')
    and r.BDATE = (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
                                    INNER JOIN PARUS.BLTBLROWS ro 
                                    on(v.PRN = ro.RN)
                                    INNER JOIN PARUS.BLSUBREPORTS s 
                                    on(ro.PRN = s.RN)
                                    INNER JOIN PARUS.BLREPORTS r
                                    on(s.PRN = r.RN)
                                    INNER JOIN PARUS.BLREPFORMED rd
                                    on(r.BLREPFORMED = rd.RN)
                                    INNER JOIN PARUS.BLREPFORM rf
                                    on(rd.PRN = rf.RN)
                                    WHERE rf.code = '38 COVID 19')
    )
    pivot
    (
    MIN(value)
    FOR POKAZATEL IN (  'Step_risk_15_MO' Step_risk_15_MO, 'Step_risk_16_dist' Step_risk_16_dist, 'Step_risk_01' Step_risk_01,
                                            'Step_risk_02' Step_risk_02, 'Step_risk_03' Step_risk_03, 'Step_risk_04' Step_risk_04, 'Step_risk_05' Step_risk_05,
                                            'Step_risk_06' Step_risk_06, 'Step_risk_07' Step_risk_07, 'Step_risk_08' Step_risk_08, 'Step_risk_09' Step_risk_09,
                                            'Step_risk_10' Step_risk_10, 'Step_risk_11' Step_risk_11, 'Step_risk_12' Step_risk_12, 'Step_risk_13' Step_risk_13,
                                            'Step_risk_14' Step_risk_14)    			
    )
    WHERE Step_risk_15_MO IS NOT NULL 
    ORDER BY Step_risk_16_dist
    """

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
