SELECT  district, lab_utr_mo, addr_pz,
                CAST(lab_utr_01 AS int) lab_utr_01 ,  lab_utr_02 , CAST(lab_utr_03 AS int) lab_utr_03,
                CAST(lab_utr_04 AS int) lab_utr_04 , CAST(lab_utr_05 AS int) lab_utr_05 , CAST(lab_utr_06 AS int) lab_utr_06,
                CAST(lab_utr_07 AS int) lab_utr_07 , CAST(lab_utr_08 AS int) lab_utr_08 , CAST(lab_utr_09 AS int) lab_utr_09,
                CAST(lab_utr_10 AS int) lab_utr_10
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
                WHERE rf.code = '26 COVID 19'
                and r.BDATE =  trunc(SYSDATE) - 1
                and i.CODE in ('district','lab_utr_MO','addr_PZ','lab_utr_01','lab_utr_02','lab_utr_03',
                        'lab_utr_04','lab_utr_05','lab_utr_06','lab_utr_07',
                        'lab_utr_08','lab_utr_09','lab_utr_10')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('district' district,'lab_utr_MO' lab_utr_mo ,'addr_PZ' addr_pz,'lab_utr_01' lab_utr_01,
                'lab_utr_02' lab_utr_02,'lab_utr_03' lab_utr_03,'lab_utr_04' lab_utr_04,'lab_utr_05' lab_utr_05,
                'lab_utr_06' lab_utr_06,'lab_utr_07' lab_utr_07,'lab_utr_08' lab_utr_08,'lab_utr_09' lab_utr_09,'lab_utr_10' lab_utr_10)
                )
             WHERE lab_utr_02 IS NOT null
        ORDER BY ORGANIZATION
