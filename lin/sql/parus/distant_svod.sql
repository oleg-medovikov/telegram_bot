SELECT DAY, 'г.Санкт-Петербург' region, ORGANIZATION, pok02,
	nvl(cast(pok03 as int),0) pok03,nvl(cast(pok04 as int),0) pok04,
	nvl(cast(pok05 as int),0) pok05,nvl(cast(pok06 as int),0) pok06,
	nvl(cast(pok07 as int),0) pok07,nvl(cast(pok08 as int),0) pok08,
	nvl(cast(pok09 as int),0) pok09,nvl(cast(pok10 as int),0) pok10,
	nvl(cast(pok11 as int),0) pok11,nvl(cast(pok12 as int),0) pok12,
	nvl(cast(pok13 as int),0) pok13,nvl(cast(pok14 as int),0) pok14,
	nvl(cast(pok15 as int),0) pok15,nvl(cast(pok16 as int),0) pok16,
	nvl(cast(pok17 as int),0) pok17,nvl(cast(pok18 as int),0) pok18,
	nvl(cast(pok19 as int),0) pok19
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
WHERE rf.CODE = 'ДистанцКонсультации'
AND r.BDATE =  trunc(SYSDATE - 1)
AND bi.CODE IN ('distanc_consult_3_02', 'distanc_consult_3_03', 'distanc_consult_3_04', 'distanc_consult_3_05',
				'distanc_consult_3_06', 'distanc_consult_3_07', 'distanc_consult_3_08', 'distanc_consult_3_09',
				'distanc_consult_3_10', 'distanc_consult_3_11', 'distanc_consult_3_12', 'distanc_consult_3_13',
				'distanc_consult_3_14', 'distanc_consult_3_15', 'distanc_consult_3_16', 'distanc_consult_3_17',
				'distanc_consult_3_18', 'distanc_consult_3_19')
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('distanc_consult_3_02' pok02, 'distanc_consult_3_03' pok03, 'distanc_consult_3_04' pok04, 'distanc_consult_3_05' pok05,
				  'distanc_consult_3_06' pok06, 'distanc_consult_3_07' pok07, 'distanc_consult_3_08' pok08, 'distanc_consult_3_09' pok09,
				  'distanc_consult_3_10' pok10, 'distanc_consult_3_11' pok11, 'distanc_consult_3_12' pok12, 'distanc_consult_3_13' pok13,
				  'distanc_consult_3_14' pok14, 'distanc_consult_3_15' pok15, 'distanc_consult_3_16' pok16, 'distanc_consult_3_17' pok17,
				  'distanc_consult_3_18' pok18, 'distanc_consult_3_19' pok19)
)
