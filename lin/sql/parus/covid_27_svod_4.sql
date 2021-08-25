SELECT   IDMO,'Количество лиц, обследованных на Covid-19 и получивших положительный результат исследования',
        nvl(cast(pok_01 as int),0) pok_01,nvl(cast(pok_02 as int),0) pok_02,nvl(cast(pok_03 as int),0) pok_03,
        nvl(cast(pok_04 as int),0) pok_04,nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,
        nvl(cast(pok_07 as int),0) pok_07,nvl(cast(pok_08 as int),0) pok_08,
        pok_09,pok_10,pok_11,pok_12,pok_13,pok_14,pok_15
FROM (
SELECT 
    to_char(r.BDATE, 'DD.MM.YYYY') day,
    a.AGNNAME ORGANIZATION ,
    rf.CODE  otchet,
    bi.CODE  pokazatel,
    CASE WHEN STRVAL IS NOT NULL THEN STRVAL 
        WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
        WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
    ELSE NULL END value
FROM PARUS.BLINDEXVALUES  d
INNER JOIN PARUS.BLSUBREPORTS s
ON (d.PRN = s.RN)
INNER JOIN PARUS.BLREPORTS r
ON(s.PRN = r.RN)
INNER JOIN PARUS.AGNLIST a
on(r.AGENT = a.rn)
INNER JOIN PARUS.BLREPFORMED pf
on(r.BLREPFORMED = pf.RN)
INNER JOIN PARUS.BLREPFORM rf 
on(pf.PRN = rf.RN)
INNER JOIN PARUS.BALANCEINDEXES bi 
on(d.BALANCEINDEX = bi.RN)
WHERE rf.CODE = '27 COVID19' 
and r.BDATE =  trunc(SYSDATE)
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('labCOVID_MO' IDMO, 'labCOVID_41' pok_01,
                    'labCOVID_42' pok_02, 'labCOVID_43' pok_03, 'labCOVID_44' pok_04,
                    'labCOVID_45' pok_05, 'labCOVID_47' pok_06, 'labCOVID_47_1' pok_07,
                    'labCOVID_47_2' pok_08, 'labCOVID_48' pok_09, 'labCOVID_49' pok_10,
                    'labCOVID_50' pok_11, 'labCOVID_51' pok_12,
                    'labCOVID_52' pok_13, 'labCOVID_53' pok_14, 'labCOVID_53_1' pok_15
                    )
)
