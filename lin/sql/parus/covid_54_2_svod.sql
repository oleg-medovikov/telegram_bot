SELECT to_char(day, 'YYYY_MM_DD') day, pok02,
    nvl(cast(pok03 as int),0)  pok03,nvl(cast(pok05 as int),0)  pok05,
    nvl(cast(pok06 as int),0)  pok06,nvl(cast(pok07 as int),0)  pok07
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
        WHERE rf.code = '54 №2 COVID 19'
        and r.BDATE =  trunc(SYSDATE)
        and i.CODE in  ('texp_test_101', 'texp_test_202','texp_test_303','texp_test_505','texp_test_606','texp_test_707')
                )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('texp_test_202' pok02, 'texp_test_303' pok03,
						  'texp_test_505' pok05, 'texp_test_606' pok06, 'texp_test_707' pok07)
        )
        WHERE pok02 IS NOT null  