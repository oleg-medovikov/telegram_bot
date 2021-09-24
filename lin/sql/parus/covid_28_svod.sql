SELECT DAY, organization,
			pok_01, pok_02, pok_03,pok_04,
			CAST(pok_05 AS int) pok_05,
			pok_06, 
			CAST(pok_07 AS int) pok_07
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
WHERE rf.code = '28 COVID 19' 
AND r.BDAte =( SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
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
				WHERE rf.code = '28 COVID 19' AND r.BDATE < trunc(SYSDATE) + 1  )	
and i.CODE in ('trud_01','trud_02','trud_03','trud_04','trud_05','trud_06','trud_07')
)
pivot
(
max(value)
FOR POKAZATEL IN ('trud_01' pok_01,'trud_02' pok_02,'trud_03' pok_03,'trud_04' pok_04,'trud_05' pok_05,'trud_06' pok_06,'trud_07' pok_07)
)
WHERE organization <> 'Наименование Контаргент используется для сводных отчетов'
