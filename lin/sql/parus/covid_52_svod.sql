SELECT ORGANIZATION, day,
CASE WHEN org IS NULL THEN 'Медицинская организация' ELSE 'Пункт вакцинации' END TYPE,
CASE WHEN org IS NULL THEN NULL ELSE substr(org ,1,INSTR(org , ' ')-1) END dist,
CASE WHEN org IS NULL THEN  ORGANIZATION ELSE substr(org ,INSTR(org , ' ')+1, LENGTH(org)) END org, vac,
        nvl(cast(pok_01 as int),0) pok_01,nvl(cast(pok_02 as int),0) pok_02,
        nvl(cast(pok_03 as int),0) pok_03,nvl(cast(pok_04 as int),0) pok_04,
        nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,
        nvl(cast(pok_08 as int),0) pok_08,nvl(cast(pok_09 as int),0) pok_09,
        nvl(cast(pok_07 as int),0) pok_07
        FROM (
        SELECT
        to_char(r.BDATE, 'DD.MM.YYYY') day,
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
        WHERE rf.code = '52 COVID 19'
        and r.BDATE =  trunc(SYSDATE-1)
         and i.CODE in ('vac_in_dist','vac_in_tvsp', 'vac_in_vac',
                        'vac_in_02','vac_in_03','vac_in_04','vac_in_05',
                        'vac_in_08','vac_in_09','vac_in_12', 'vac_in_13', 'vac_in_14')
        )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('vac_in_dist' dist,'vac_in_tvsp' org, 'vac_in_vac' vac,
                        'vac_in_02' pok_01,'vac_in_03' pok_02,'vac_in_04' pok_03,
                        'vac_in_05' pok_04,'vac_in_08' pok_05,'vac_in_09' pok_06,
                        'vac_in_12' pok_07, 'vac_in_13' pok_08,'vac_in_14' pok_09
                           )
        )
ORDER BY ORGANIZATION,TYPE
