SELECT to_char(day, 'YYYY_MM_DD') day, pok01,pok02,
    nvl(cast(pok03 as int),0)  pok03,nvl(cast(pok05 as int),0)  pok05,
    nvl(cast(pok06 as int),0)  pok06
FROM (
        SELECT
            r.BDATE day,a.AGNNAME organization,i.CODE pokazatel,ro.NUMB row_index,
            CASE WHEN STRVAL  IS NOT NULL THEN STRVAL
                 WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(255))
                 WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(255))
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
        WHERE rf.code = '54 COVID 19'
        and r.BDATE =  trunc(SYSDATE-1)
        and i.CODE in  ('exp_test_01', 'exp_test_02','exp_test_03','exp_test_05','exp_test_06')
                )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('exp_test_01' pok01, 'exp_test_02' pok02, 'exp_test_03' pok03,
						  'exp_test_05' pok05, 'exp_test_06' pok06)
        )            
