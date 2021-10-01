SELECT DAY, organization, podv, MO,
			pok_01, pok_02,
            cast(pok_03 as int) pok_03,pok_04,
			cast(pok_05 as int) pok_05
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
WHERE rf.code = '41 COVID 19'
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
				WHERE rf.code = '41 COVID 19' AND r.BDATE < trunc(SYSDATE) + 2  )	
and i.CODE in ('stud_hniz_PODV','stud_hniz_MO' ,'stud_hniz_01','stud_hniz_02','stud_hniz_03','stud_hniz_04','stud_hniz_05')
)
pivot
(
max(value)
FOR POKAZATEL IN ('stud_hniz_PODV' podv, 'stud_hniz_MO' MO, 'stud_hniz_01' pok_01,'stud_hniz_02' pok_02,'stud_hniz_03' pok_03,'stud_hniz_04' pok_04,'stud_hniz_05' pok_05)
)
WHERE organization <> 'Наименование Контаргент используется для сводных отчетов' AND MO IS NOT null
