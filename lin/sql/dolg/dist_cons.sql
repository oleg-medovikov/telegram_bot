SELECT DISTINCT a.AGNNAME ORGANIZATION
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
