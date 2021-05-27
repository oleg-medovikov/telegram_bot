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
                                        WHERE rf.code = '36 COVID 19'
                                        AND r.Bdate < SYSDATE + 2))
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
