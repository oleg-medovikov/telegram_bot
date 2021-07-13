SELECT 1 AS INDX, ORGANIZATION,'Всего' as typevacine, concat(ORGANIZATION, ' Всего') AS scep
		,nvl(cast(cov_02 as int),0) cov_02
		,nvl(cast(cov_03 as int),0) cov_03,nvl(cast(cov_04 as int),0) cov_04
		,nvl(cast(cov_05 as int),0) cov_05,nvl(cast(cov_06 as int),0) cov_06
		,nvl(cast(cov_07 as int),0) cov_07,nvl(cast(cov_08 as int),0) cov_08
		,nvl(cast(cov_09 as int),0) cov_09,nvl(cast(cov_10 as int),0) cov_10
		,nvl(cast(cov_11 as int),0) cov_11,nvl(cast(cov_12 as int),0) cov_12
		,nvl(cast(cov_13 as int),0) cov_13,nvl(cast(cov_14 as int),0) cov_14
		,nvl(cast(cov_15 as int),0) cov_15,nvl(cast(cov_16 as int),0) cov_16
		,nvl(cast(cov_17 as int),0) cov_17,nvl(cast(cov_18 as int),0) cov_18
		,nvl(cast(cov_19 as int),0) cov_19
		FROM (
		SELECT
			to_char(r.BDATE, 'DD.MM.YYYY') day,
			a.AGNNAME ORGANIZATION ,
			a.ADDR_DISTRICT_RN dist, 
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
		WHERE rf.code = '40 COVID 19'
			and  r.BDATE =  trunc(SYSDATE) - 1
			and bi.CODE in ('revac_02_01','revac_03_01','revac_04_01','revac_05_01','revac_06_01','revac_07_01','revac_08_01'
							,'revac_09_01','revac_10_01','revac_11_01','revac_12_01','revac_13_01','revac_14_01','revac_15_01'
							,'revac_16_01','revac_17_01','revac_18_01','revac_19')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_01' cov_02,'revac_03_01' cov_03,'revac_04_01' cov_04,'revac_05_01' cov_05,
					'revac_06_01' cov_06,'revac_07_01' cov_07,'revac_08_01' cov_08
					,'revac_09_01' cov_09,'revac_10_01' cov_10,'revac_11_01' cov_11,
					'revac_12_01' cov_12,'revac_13_01' cov_13,'revac_14_01' cov_14,'revac_15_01' cov_15
					,'revac_16_01' cov_16,'revac_17_01' cov_17,'revac_18_01' cov_18,'revac_19_01' cov_19)
		)
UNION all
SELECT 2 AS INDX, ORGANIZATION,'Гам-КОВИД-Вак (Спутник-V)' as  typevacine, concat(ORGANIZATION, ' Гам-КОВИД-Вак (Спутник-V)') AS scep
		,nvl(cast(cov_02 as int),0) cov_02
		,nvl(cast(cov_03 as int),0) cov_03,nvl(cast(cov_04 as int),0) cov_04
		,nvl(cast(cov_05 as int),0) cov_05,nvl(cast(cov_06 as int),0) cov_06
		,nvl(cast(cov_07 as int),0) cov_07,nvl(cast(cov_08 as int),0) cov_08
		,nvl(cast(cov_09 as int),0) cov_09,nvl(cast(cov_10 as int),0) cov_10
		,nvl(cast(cov_11 as int),0) cov_11,nvl(cast(cov_12 as int),0) cov_12
		,nvl(cast(cov_13 as int),0) cov_13,nvl(cast(cov_14 as int),0) cov_14
		,nvl(cast(cov_15 as int),0) cov_15,nvl(cast(cov_16 as int),0) cov_16
		,nvl(cast(cov_17 as int),0) cov_17,nvl(cast(cov_18 as int),0) cov_18
		,nvl(cast(cov_19 as int),0) cov_19
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
		WHERE rf.code = '40 COVID 19'
			and  r.BDATE =  trunc(SYSDATE) - 1
			and bi.CODE in ('revac_02_02','revac_03_02','revac_04_02','revac_05_02','revac_06_02','revac_07_02','revac_08_02'
							,'revac_09_02','revac_10_02','revac_11_02','revac_12_02','revac_13_02','revac_14_02','revac_15_02'
							,'revac_16_02','revac_17_02','revac_18_02','revac_19')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_02' cov_02,'revac_03_02' cov_03,'revac_04_02' cov_04,'revac_05_02' cov_05,
					'revac_06_02' cov_06,'revac_07_02' cov_07,'revac_08_02' cov_08
					,'revac_09_02' cov_09,'revac_10_02' cov_10,'revac_11_02' cov_11,
					'revac_12_02' cov_12,'revac_13_02' cov_13,'revac_14_02' cov_14,'revac_15_02' cov_15
					,'revac_16_02' cov_16,'revac_17_02' cov_17,'revac_18_02' cov_18,'revac_19_02' cov_19)
		)
UNION ALL
SELECT 3 AS INDX,ORGANIZATION,'КовиВак' as typevacine, concat(ORGANIZATION, ' КовиВак') AS scep
		,nvl(cast(cov_02 as int),0) cov_02
		,nvl(cast(cov_03 as int),0) cov_03,nvl(cast(cov_04 as int),0) cov_04
		,nvl(cast(cov_05 as int),0) cov_05,nvl(cast(cov_06 as int),0) cov_06
		,nvl(cast(cov_07 as int),0) cov_07,nvl(cast(cov_08 as int),0) cov_08
		,nvl(cast(cov_09 as int),0) cov_09,nvl(cast(cov_10 as int),0) cov_10
		,nvl(cast(cov_11 as int),0) cov_11,nvl(cast(cov_12 as int),0) cov_12
		,nvl(cast(cov_13 as int),0) cov_13,nvl(cast(cov_14 as int),0) cov_14
		,nvl(cast(cov_15 as int),0) cov_15,nvl(cast(cov_16 as int),0) cov_16
		,nvl(cast(cov_17 as int),0) cov_17,nvl(cast(cov_18 as int),0) cov_18
		,nvl(cast(cov_19 as int),0) cov_19
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
		WHERE rf.code = '40 COVID 19'
			and  r.BDATE =  trunc(SYSDATE) - 1
			and bi.CODE in ('revac_02_03','revac_03_03','revac_04_03','revac_05_03','revac_06_03','revac_07_03','revac_08_03'
							,'revac_09_03','revac_10_03','revac_11_03','revac_12_03','revac_13_03','revac_14_03','revac_15_03'
							,'revac_16_03','revac_17_03','revac_18_03','revac_19')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_03' cov_02,'revac_03_03' cov_03,'revac_04_03' cov_04,'revac_05_03' cov_05,
					'revac_06_03' cov_06,'revac_07_03' cov_07,'revac_08_03' cov_08
					,'revac_09_03' cov_09,'revac_10_03' cov_10,'revac_11_03' cov_11,
					'revac_12_03' cov_12,'revac_13_03' cov_13,'revac_14_03' cov_14,'revac_15_03' cov_15
					,'revac_16_03' cov_16,'revac_17_03' cov_17,'revac_18_03' cov_18,'revac_19_03' cov_19)
		)
UNION ALL 
SELECT 4 AS INDX,ORGANIZATION,'ЭпиВакКорона' as typevacine, concat(ORGANIZATION, ' ЭпиВакКорона') AS scep
		,nvl(cast(cov_02 as int),0) cov_02
		,nvl(cast(cov_03 as int),0) cov_03,nvl(cast(cov_04 as int),0) cov_04
		,nvl(cast(cov_05 as int),0) cov_05,nvl(cast(cov_06 as int),0) cov_06
		,nvl(cast(cov_07 as int),0) cov_07,nvl(cast(cov_08 as int),0) cov_08
		,nvl(cast(cov_09 as int),0) cov_09,nvl(cast(cov_10 as int),0) cov_10
		,nvl(cast(cov_11 as int),0) cov_11,nvl(cast(cov_12 as int),0) cov_12
		,nvl(cast(cov_13 as int),0) cov_13,nvl(cast(cov_14 as int),0) cov_14
		,nvl(cast(cov_15 as int),0) cov_15,nvl(cast(cov_16 as int),0) cov_16
		,nvl(cast(cov_17 as int),0) cov_17,nvl(cast(cov_18 as int),0) cov_18
		,nvl(cast(cov_19 as int),0) cov_19
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
		WHERE rf.code = '40 COVID 19'
			and  r.BDATE =  trunc(SYSDATE) - 1
			and bi.CODE in ('revac_02_04','revac_03_04','revac_04_04','revac_05_04','revac_06_04','revac_07_04','revac_08_04'
							,'revac_09_04','revac_10_04','revac_11_04','revac_12_04','revac_13_04','revac_14_04','revac_15_04'
							,'revac_16_04','revac_17_04','revac_18_04','revac_19')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_04' cov_02,'revac_03_04' cov_03,'revac_04_04' cov_04,'revac_05_04' cov_05,
					'revac_06_04' cov_06,'revac_07_04' cov_07,'revac_08_04' cov_08
					,'revac_09_04' cov_09,'revac_10_04' cov_10,'revac_11_04' cov_11,
					'revac_12_04' cov_12,'revac_13_04' cov_13,'revac_14_04' cov_14,'revac_15_04' cov_15
					,'revac_16_04' cov_16,'revac_17_04' cov_17,'revac_18_04' cov_18,'revac_19_04' cov_19)
		)
UNION ALL 
SELECT 5 AS INDX, ORGANIZATION,'Спутник Лайт' as typevacine, concat(ORGANIZATION, ' Спутник Лайт') AS scep
		,nvl(cast(cov_02 as int),0) cov_02
		,nvl(cast(cov_03 as int),0) cov_03,nvl(cast(cov_04 as int),0) cov_04
		,nvl(cast(cov_05 as int),0) cov_05,nvl(cast(cov_06 as int),0) cov_06
		,nvl(cast(cov_07 as int),0) cov_07,nvl(cast(cov_08 as int),0) cov_08
		,nvl(cast(cov_09 as int),0) cov_09,nvl(cast(cov_10 as int),0) cov_10
		,nvl(cast(cov_11 as int),0) cov_11,nvl(cast(cov_12 as int),0) cov_12
		,nvl(cast(cov_13 as int),0) cov_13,nvl(cast(cov_14 as int),0) cov_14
		,nvl(cast(cov_15 as int),0) cov_15,0 as cov_16, 0 as cov_17
		,nvl(cast(cov_18 as int),0) cov_18,nvl(cast(cov_19 as int),0) cov_19
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
		WHERE rf.code = '40 COVID 19'
			and  r.BDATE =  trunc(SYSDATE) - 1
			and bi.CODE in ('revac_02_05','revac_03_05','revac_04_05','revac_05_05','revac_06_05','revac_07_05','revac_08_05'
							,'revac_09_05','revac_10_05','revac_11_05','revac_12_05','revac_13_05','revac_14_05','revac_15_05'
							,'revac_16_05','revac_17_05','revac_18_05','revac_19')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_05' cov_02,'revac_03_05' cov_03,'revac_04_05' cov_04,'revac_05_05' cov_05,
					'revac_06_05' cov_06,'revac_07_05' cov_07,'revac_08_05' cov_08
					,'revac_09_05' cov_09,'revac_10_05' cov_10,'revac_11_05' cov_11,
					'revac_12_05' cov_12,'revac_13_05' cov_13,'revac_14_05' cov_14,'revac_15_05' cov_15
					,'revac_18_05' cov_18,'revac_19_05' cov_19)
		)
ORDER BY ORGANIZATION, indx
