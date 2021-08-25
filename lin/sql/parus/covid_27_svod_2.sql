SELECT   IDMO,nvl(cast(pok_00 as int),0) pok_00,
        nvl(cast(pok_01 as int),0) pok_01,nvl(cast(pok_02 as int),0) pok_02,nvl(cast(pok_03 as int),0) pok_03,
        nvl(cast(pok_04 as int),0) pok_04,nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,
        nvl(cast(pok_07 as int),0) pok_07,nvl(cast(pok_08 as int),0) pok_08,nvl(cast(pok_09 as int),0) pok_09,
        nvl(cast(pok_10 as int),0) pok_10,nvl(cast(pok_11 as int),0) pok_11,
        nvl(cast(pok_12 as int),0) pok_12,nvl(cast(pok_13 as int),0) pok_13,nvl(cast(pok_14 as int),0) pok_14,
        nvl(cast(pok_15 as int),0) pok_15,nvl(cast(pok_16 as int),0) pok_16,nvl(cast(pok_17 as int),0) pok_17,
        nvl(cast(pok_18 as int),0) pok_18,nvl(cast(pok_19 as int),0) pok_19,nvl(cast(pok_20 as int),0) pok_20,
        nvl(cast(pok_201 as int),0) pok_201,nvl(cast(pok_21 as int),0) pok_21
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
FOR POKAZATEL IN ('labCOVID_MO' IDMO, 'labCOVID_18' pok_00, 'labCOVID_18_01' pok_01,
                    'labCOVID_18_02' pok_02, 'labCOVID_18_03' pok_03, 'labCOVID_18_04' pok_04,
                    'labCOVID_18_05' pok_05, 'labCOVID_18_06' pok_06, 'labCOVID_18_07' pok_07,
                    'labCOVID_18_08' pok_08, 'labCOVID_18_09' pok_09, 'labCOVID_18_10' pok_10,
                    'labCOVID_18_11' pok_11, 'labCOVID_18_12' pok_12,
                    'labCOVID_18_13' pok_13, 'labCOVID_18_14' pok_14, 'labCOVID_18_15' pok_15,
                    'labCOVID_18_16' pok_16, 'labCOVID_18_17' pok_17, 'labCOVID_18_18' pok_18,
                    'labCOVID_18_19' pok_19, 'labCOVID_18_20' pok_20, 'labCOVID_16_1' pok_201,
                    'labCOVID_18_21' pok_21
                    )
)

