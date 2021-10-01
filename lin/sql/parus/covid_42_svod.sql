SELECT DAY, organization, podv, MO,
			pok_01, CAST(pok_02 AS int) pok_02, CAST(pok_03 AS int) pok_03,
			CAST(pok_04 AS int) pok_04,CAST(pok_05 AS int) pok_05
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
WHERE rf.code = '42 COVID 19'
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
				WHERE rf.code = '42 COVID 19' AND r.BDATE < trunc(SYSDATE) + 2  )	
and i.CODE in ('volonter_06_PODV','volonter_07_MO' ,'volonter_01','volonter_02','volonter_03','volonter_04','volonter_05')
)
pivot
(
max(value)
FOR POKAZATEL IN ('volonter_06_PODV' podv, 'volonter_07_MO' MO, 'volonter_01' pok_01,'volonter_02' pok_02,'volonter_03' pok_03,'volonter_04' pok_04,'volonter_05' pok_05)
)
WHERE organization <> 'Наименование Контаргент используется для сводных отчетов' AND MO IS NOT null
