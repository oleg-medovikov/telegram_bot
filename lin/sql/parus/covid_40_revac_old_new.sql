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
			and  r.BDATE =  trunc(SYSDATE) - 2
			and bi.CODE in ('revac_02_01_s','revac_03_01_s','revac_04_01_s','revac_05_01_s','revac_06_01_s','revac_07_01_s','revac_08_01_s'
							,'revac_09_01_s','revac_10_01_s','revac_11_01_s','revac_12_01_s','revac_13_01_s','revac_14_01_s','revac_15_01_s'
							,'revac_16_01_s','revac_17_01_s','revac_18_01_s','revac_19_01_s')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_01_s' cov_02,'revac_03_01_s' cov_03,'revac_04_01_s' cov_04,'revac_05_01_s' cov_05,
					'revac_06_01_s' cov_06,'revac_07_01_s' cov_07,'revac_08_01_s' cov_08
					,'revac_09_01_s' cov_09,'revac_10_01_s' cov_10,'revac_11_01_s' cov_11,
					'revac_12_01_s' cov_12,'revac_13_01_s' cov_13,'revac_14_01_s' cov_14,'revac_15_01_s' cov_15
					,'revac_16_01_s' cov_16,'revac_17_01_s' cov_17,'revac_18_01_s' cov_18,'revac_19_01_s' cov_19)
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
			and  r.BDATE =  trunc(SYSDATE) - 2
			and bi.CODE in ('revac_02_02_s','revac_03_02_s','revac_04_02_s','revac_05_02_s','revac_06_02_s','revac_07_02_s','revac_08_02_s'
							,'revac_09_02_s','revac_10_02_s','revac_11_02_s','revac_12_02_s','revac_13_02_s','revac_14_02_s','revac_15_02_s'
							,'revac_16_02_s','revac_17_02_s','revac_18_02_s','revac_19_02_s')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_02_s' cov_02,'revac_03_02_s' cov_03,'revac_04_02_s' cov_04,'revac_05_02_s' cov_05,
					'revac_06_02_s' cov_06,'revac_07_02_s' cov_07,'revac_08_02_s' cov_08
					,'revac_09_02_s' cov_09,'revac_10_02_s' cov_10,'revac_11_02_s' cov_11,
					'revac_12_02_s' cov_12,'revac_13_02_s' cov_13,'revac_14_02_s' cov_14,'revac_15_02_s' cov_15
					,'revac_16_02_s' cov_16,'revac_17_02_s' cov_17,'revac_18_02_s' cov_18,'revac_19_02_s' cov_19)
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
			and  r.BDATE =  trunc(SYSDATE) - 2
			and bi.CODE in ('revac_02_03_s','revac_03_03_s','revac_04_03_s','revac_05_03_s','revac_06_03_s','revac_07_03_s','revac_08_03_s'
							,'revac_09_03_s','revac_10_03_s','revac_11_03_s','revac_12_03_s','revac_13_03_s','revac_14_03_s','revac_15_03_s'
							,'revac_16_03_s','revac_17_03_s','revac_18_03_s','revac_19_03_s')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_03_s' cov_02,'revac_03_03_s' cov_03,'revac_04_03_s' cov_04,'revac_05_03_s' cov_05,
					'revac_06_03_s' cov_06,'revac_07_03_s' cov_07,'revac_08_03_s' cov_08
					,'revac_09_03_s' cov_09,'revac_10_03_s' cov_10,'revac_11_03_s' cov_11,
					'revac_12_03_s' cov_12,'revac_13_03_s' cov_13,'revac_14_03_s' cov_14,'revac_15_03_s' cov_15
					,'revac_16_03_s' cov_16,'revac_17_03_s' cov_17,'revac_18_03_s' cov_18,'revac_19_03_s' cov_19)
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
			and  r.BDATE =  trunc(SYSDATE) - 2
			and bi.CODE in ('revac_02_04_s','revac_03_04_s','revac_04_04_s','revac_05_04_s','revac_06_04_s','revac_07_04_s','revac_08_04_s'
							,'revac_09_04_s','revac_10_04_s','revac_11_04_s','revac_12_04_s','revac_13_04_s','revac_14_04_s','revac_15_04_s'
							,'revac_16_04_s','revac_17_04_s','revac_18_04_s','revac_19_04_s')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_04_s' cov_02,'revac_03_04_s' cov_03,'revac_04_04_s' cov_04,'revac_05_04_s' cov_05,
					'revac_06_04_s' cov_06,'revac_07_04_s' cov_07,'revac_08_04_s' cov_08
					,'revac_09_04_s' cov_09,'revac_10_04_s' cov_10,'revac_11_04_s' cov_11,
					'revac_12_04_s' cov_12,'revac_13_04_s' cov_13,'revac_14_04_s' cov_14,'revac_15_04_s' cov_15
					,'revac_16_04_s' cov_16,'revac_17_04_s' cov_17,'revac_18_04_s' cov_18,'revac_19_04_s' cov_19)
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
			and  r.BDATE =  trunc(SYSDATE) - 2
			and bi.CODE in ('revac_02_05_s','revac_03_05_s','revac_04_05_s','revac_05_05_s','revac_06_05_s','revac_07_05_s','revac_08_05_s'
							,'revac_09_05_s','revac_10_05_s','revac_11_05_s','revac_12_05_s','revac_13_05_s','revac_14_05_s','revac_15_05_s'
							,'revac_16_05_s','revac_17_05_s','revac_18_05_s','revac_19_05_s')
		)
	pivot
		(
	max(value)
	FOR POKAZATEL IN ('revac_02_05_s' cov_02,'revac_03_05_s' cov_03,'revac_04_05_s' cov_04,'revac_05_05_s' cov_05,
					'revac_06_05_s' cov_06,'revac_07_05_s' cov_07,'revac_08_05_s' cov_08
					,'revac_09_05_s' cov_09,'revac_10_05_s' cov_10,'revac_11_05_s' cov_11,
					'revac_12_05_s' cov_12,'revac_13_05_s' cov_13,'revac_14_05_s' cov_14,'revac_15_05_s' cov_15
					,'revac_18_05_s' cov_18,'revac_19_05_s' cov_19)
		)
ORDER BY ORGANIZATION, indx

