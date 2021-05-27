SELECT  covid_02, covid_03,
                        cast(covid_04 AS int) covid_04, 
                        CASE 
                          WHEN (covid_03 = 'СПб ГБУЗ "Александровская больница"') AND (cast(covid_05 AS int) < 277)
                            THEN 277
                            ELSE cast(covid_05 AS int) 
                        END covid_05, cast(covid_06_old_2 AS int) covid_06_old_2 ,
                        cast(covid_06 AS int) covid_06 , 
                        CASE 
                          WHEN (covid_03 = 'СПб ГБУЗ "Александровская больница"') AND (cast(covid_07 AS int) < 252)
                            THEN 252
                            ELSE cast(covid_07 AS int) 
                        END covid_07,
                        cast(covid_08_old_2 AS int) covid_08_old_2, 
                        cast(covid_08 AS int) covid_08 , cast(covid_09 AS int) covid_09, 
                        CASE 
                          WHEN (covid_03 = 'СПб ГБУЗ "Александровская больница"') AND (cast(covid_09_2 AS int) < 1020)
                            THEN 1020
                            ELSE cast(covid_09_2 AS int) 
                        END covid_09_2, 
                        cast(covid_10_old_2 AS int) covid_10_old_2,
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
        AND r.BDATE =  trunc(SYSDATE - 1) )
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
        )
