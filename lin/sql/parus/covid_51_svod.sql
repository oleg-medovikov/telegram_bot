SELECT day, cov_02
		,nvl(cast(cov_04 as int),0) cov_04
		,nvl(cast(cov_05 as int),0) cov_05
		,nvl(cast(cov_06 as int),0) cov_06
		,nvl(cast(cov_08 as int),0) cov_08
		,nvl(cast(cov_10 as int),0) cov_10
		,nvl(cast(cov_11 as int),0) cov_11
		,nvl(cast(cov_12 as int),0) cov_12
		,nvl(cast(cov_13 as int),0) cov_13
		,(nvl(cast(cov_08 as int),0) + nvl(cast(cov_11 as int),0) + nvl(cast(cov_13 as int),0)) cov_sum
		,round((nvl(cast(cov_08 as int),0) + nvl(cast(cov_11 as int),0) + nvl(cast(cov_13 as int),0))/cast(cov_04 as int)*100, 2) cov_procent
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
		WHERE rf.code = '51 COVID 19'
			and  r.BDATE =  trunc(SYSDATE)
			and bi.CODE in ('51_cov_02','51_cov_04','51_cov_05','51_cov_06','51_cov_08'
							,'51_cov_10','51_cov_11','51_cov_12','51_cov_13')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('51_cov_02' cov_02,'51_cov_04' cov_04,'51_cov_05' cov_05
					  ,'51_cov_06' cov_06,'51_cov_08' cov_08,'51_cov_10' cov_10
					  ,'51_cov_11' cov_11,'51_cov_12' cov_12,'51_cov_13' cov_13
	))
ORDER BY ORGANIZATION
