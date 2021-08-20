SELECT 
	dan.*
	FROM (SELECT DAY
				,cov_02
				,nvl(cast(cov_03 as int),0) cov_03
				,nvl(cast(cov_04 as int),0) cov_04
				,nvl(cast(cov_05 as int),0) cov_05
				,nvl(cast(cov_06 as int),0) cov_06
				,nvl(cast(cov_07 as int),0) cov_07
				,nvl(cast(cov_08 as int),0) cov_08
				,nvl(cast(cov_09 as int),0) cov_09
				,nvl(cast(cov_10 as int),0) cov_10
				,nvl(cast(cov_11 as int),0) cov_11
				,nvl(cast(cov_12 as int),0) cov_12
				,nvl(cast(cov_13 as int),0) cov_13
				,nvl(cast(cov_14 as int),0) cov_14
				,cov_15
				,RN
			FROM (SELECT to_char(r.BDATE, 'DD.MM.YYYY') DAY
						,a.AGNNAME ORGANIZATION
						,a.RN
						,rf.CODE otchet
						,bi.CODE pokazatel
						,CASE 
							WHEN STRVAL IS NOT NULL 
								THEN STRVAL
							WHEN NUMVAL IS NOT NULL 
								THEN CAST(NUMVAL  AS varchar(30))
							WHEN DATEVAL IS NOT NULL
								THEN CAST(DATEVAL AS varchar(30))
								ELSE NULL 
						END value
					FROM PARUS.BLINDEXVALUES  d
						INNER JOIN PARUS.BLSUBREPORTS s
							ON (d.PRN = s.RN)
						INNER JOIN PARUS.BLREPORTS r
							ON (s.PRN = r.RN)
						INNER JOIN PARUS.AGNLIST a
							ON (r.AGENT = a.rn)
						INNER JOIN PARUS.BLREPFORMED pf
							ON (r.BLREPFORMED = pf.RN)
						INNER JOIN PARUS.BLREPFORM rf
							ON (pf.PRN = rf.RN)
						INNER JOIN PARUS.BALANCEINDEXES bi
							ON (d.BALANCEINDEX = bi.RN)
					WHERE rf.code = '51 COVID 19'
						  AND bi.CODE IN ('51_cov_02','51_cov_03','51_cov_04','51_cov_05','51_cov_06','51_cov_07','51_cov_08'
						  				 ,'51_cov_09','51_cov_10','51_cov_11','51_cov_12','51_cov_13','51_cov_14','51_cov_15'))
			PIVOT (MAX(value)
						FOR POKAZATEL IN ('51_cov_02' cov_02,'51_cov_03' cov_03,'51_cov_04' cov_04,'51_cov_05' cov_05
										  ,'51_cov_06' cov_06,'51_cov_07' cov_07,'51_cov_08' cov_08,'51_cov_09' cov_09
										  ,'51_cov_10' cov_10,'51_cov_11' cov_11,'51_cov_12' cov_12,'51_cov_13' cov_13
										  ,'51_cov_14' cov_14,'51_cov_15' cov_15))) dan
		INNER JOIN (SELECT to_char(MAX(DAY), 'DD.MM.YYYY')  DAY
									,RN
						FROM (SELECT r.BDATE DAY
									,a.RN
								FROM PARUS.BLINDEXVALUES  d
									INNER JOIN PARUS.BLSUBREPORTS s
										ON (d.PRN = s.RN)
									INNER JOIN PARUS.BLREPORTS r
										ON (s.PRN = r.RN)
									INNER JOIN PARUS.AGNLIST a
										ON (r.AGENT = a.rn)
									INNER JOIN PARUS.BLREPFORMED pf
										ON (r.BLREPFORMED = pf.RN)
									INNER JOIN PARUS.BLREPFORM rf
										ON (pf.PRN = rf.RN)
								WHERE rf.code = '51 COVID 19') dan1
						GROUP BY RN) list /*Выбираем все даты последних отчетов*/
			ON (list.DAY = dan.DAY
				AND list.RN = dan.RN)	
	ORDER BY list.DAY DESC, dan.cov_02 ASC