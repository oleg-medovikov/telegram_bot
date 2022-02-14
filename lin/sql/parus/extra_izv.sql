SELECT to_char(day, 'YYYY_MM_DD') day, pok01,
    nvl(cast(pok02 as int),0)  pok02,nvl(cast(pok03 as int),0)  pok03
FROM (
        SELECT
			r.BDATE day,
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
		WHERE rf.code = 'ЭкстренныеИзвещения'
        and r.BDATE =  (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
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
				WHERE rf.code = 'ЭкстренныеИзвещения' AND r.BDATE < trunc(SYSDATE) + 1  )
        and bi.CODE in  ('extra_izv_01', 'extra_izv_02','extra_izv_03')
                )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('extra_izv_01' pok01, 'extra_izv_02' pok02, 'extra_izv_03' pok03)
        )            
