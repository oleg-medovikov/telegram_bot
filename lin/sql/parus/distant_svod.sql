SELECT DAY, 'г.Санкт-Петербург' region, ORGANIZATION, POK02,
	nvl(cast(pok03 as int),0) pok03, nvl(cast(pok04 as int),0) pok04,
	nvl(cast(pok05 as int),0) pok05, nvl(cast(pok06 as int),0) pok06,
	nvl(cast(pok07 as int),0) pok07, nvl(cast(pok08 as int),0) pok08,
	nvl(cast(pok09 as int),0) pok09, nvl(cast(pok10 as int),0) pok10,
	nvl(cast(pok11 as int),0) pok11, nvl(cast(pok13 as int),0) pok13,
	nvl(cast(pok14 as int),0) pok14, nvl(cast(pok15 as int),0) pok15,
	nvl(cast(pok16 as int),0) pok16,nvl(cast(pok19 as int),0) pok19,
       	nvl(cast(pok17 as int),0) pok17,nvl(cast(pok18 as int),0) pok18
	FROM (
        SELECT
                to_char(r.BDATE, 'YYYY_MM_DD') day,
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
        WHERE rf.code = 'ДистанцКонсультации'
        and r.BDATE =  trunc(SYSDATE - 1)
        and i.CODE IN ('distanc_consult_2_02', 'distanc_consult_2_03', 'distanc_consult_2_04', 'distanc_consult_2_05',
        		       'distanc_consult_2_06', 'distanc_consult_2_07', 'distanc_consult_2_08', 'distanc_consult_2_09',
        		       'distanc_consult_2_10', 'distanc_consult_2_11', 'distanc_consult_2_13', 'distanc_consult_2_14',
        		       'distanc_consult_2_15', 'distanc_consult_2_16', 'distanc_consult_2_19', 'distanc_consult_2_17', 'distanc_consult_2_18')
        		       )
		pivot
        (
        max(value)
        FOR POKAZATEL IN (
        'distanc_consult_2_02' pok02, 'distanc_consult_2_03' pok03, 'distanc_consult_2_04' pok04, 'distanc_consult_2_05' pok05,
       	'distanc_consult_2_06' pok06, 'distanc_consult_2_07' pok07, 'distanc_consult_2_08' pok08, 'distanc_consult_2_09' pok09,
        'distanc_consult_2_10' pok10, 'distanc_consult_2_11' pok11, 'distanc_consult_2_13' pok13, 'distanc_consult_2_14' pok14,
        'distanc_consult_2_15' pok15, 'distanc_consult_2_16' pok16, 'distanc_consult_2_19' pok19, 'distanc_consult_2_17' pok17, 'distanc_consult_2_18' pok18
)) WHERE POK02 IS NOT NULL 
