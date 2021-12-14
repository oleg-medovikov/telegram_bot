SELECT  day, ORGANIZATION, 
		nvl(cast(pok1 as int),0) pok1,nvl(cast(pok2 as int),0) pok2,
		nvl(cast(pok3 as int),0) pok3,nvl(cast(pok4 as int),0) pok4,
		nvl(cast(pok5 as int),0) pok5,
		pok7,pok8  --pok6
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
WHERE rf.CODE = '4.3 COVID 19 Стац.' 
and r.BDATE = ( SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
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
				WHERE rf.code = '4.3 COVID 19 Стац.' AND r.BDATE < trunc(SYSDATE))
--and bi.CODE IN ('4.3_1_01','4.3_2_01','4.3_3_01','4.3_4_01','4.3_5_01','4.3_6_01','4.3_7_01','4.3_8_01')
order by  d.BALANCEINDEX 
)
pivot
(
MIN(value)
FOR POKAZATEL IN (pokazateli)
)
where ORGANIZATION <> 'Наименование Контаргент используется для сводных отчетов'
ORDER BY pok1
