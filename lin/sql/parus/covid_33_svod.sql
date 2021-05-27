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
				WHERE rf.code = '33 COVID 19'
                                AND r.Bdate < SYSDATE + 2
                                ) 
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
