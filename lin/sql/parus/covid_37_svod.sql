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
                                    WHERE rf.code = '37 COVID 19'
                                    AND r.Bdate < SYSDATE + 2))
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
