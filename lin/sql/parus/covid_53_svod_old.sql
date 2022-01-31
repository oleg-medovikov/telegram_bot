SELECT to_char(day, 'YYYY_MM_DD') day,ORGANIZATION,'Пункт вакцинации' type,POK02,POK03,
    nvl(cast(pok04 as int),0)  pok04,nvl(cast(pok05 as int),0)  pok05,
    nvl(cast(pok07 as int),0)  pok07,nvl(cast(pok49 as int),0)  pok49,
    nvl(cast(pok51 as int),0)  pok51,nvl(cast(pok16 as int),0)  pok16,
    nvl(cast(pok18 as int),0)  pok18,nvl(cast(pok37 as int),0)  pok37,
    nvl(cast(pok39 as int),0)  pok39,nvl(cast(pok52 as int),0)  pok52,
    nvl(cast(pok54 as int),0)  pok54,nvl(cast(pok40 as int),0)  pok40,
    nvl(cast(pok42 as int),0)  pok42,nvl(cast(pok43 as int),0)  pok43,
    nvl(cast(pok45 as int),0)  pok45,nvl(cast(pok47 as int),0)  pok47,
    nvl(cast(pok48 as int),0)  pok48
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
        WHERE rf.code = '53 COVID 19'
        and r.BDATE =  trunc(SYSDATE-2)
        and i.CODE in  ('vac_det_01','vac_det_02','vac_det_03','vac_det_04','vac_det_05','vac_det_07','vac_det_49',
        				'vac_det_51','vac_det_16','vac_det_18','vac_det_37','vac_det_39','vac_det_52','vac_det_54',
        				'vac_det_40','vac_det_42','vac_det_43','vac_det_45','vac_det_47','vac_det_48')
                )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('vac_det_01' pok01,'vac_det_02' pok02,'vac_det_03' pok03,'vac_det_04' pok04,'vac_det_05' pok05,
        				  'vac_det_07' pok07,'vac_det_49' pok49,'vac_det_51' pok51,'vac_det_16' pok16,'vac_det_18' pok18,
                          'vac_det_37' pok37,'vac_det_39' pok39,'vac_det_52' pok52,'vac_det_54' pok54,'vac_det_40' pok40,
                          'vac_det_42' pok42,'vac_det_43' pok43,'vac_det_45' pok45,'vac_det_47' pok47,'vac_det_48' pok48)
        )            
        WHERE POK01 IS NOT NULL
