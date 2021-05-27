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
                                    WHERE rf.code = '38 COVID 19'
                                    AND r.Bdate < SYSDATE + 2 ))
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
