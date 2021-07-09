SELECT  day, disrict,  podch, ORGANIZATION,  '1. Ежедневная (суточная) потребность **' as TYPE,
		nvl(cast(pok_02 as int),0) pok_02,nvl(cast(pok_03 as int),0) pok_03,nvl(cast(pok_04 as int),0) pok_04,
		nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,nvl(cast(pok_07 as int),0) pok_07,
		nvl(cast(pok_08 as int),0) pok_08,nvl(cast(pok_09 as int),0) pok_09,nvl(cast(pok_10 as int),0) pok_10,
		nvl(cast(pok_11 as int),0) pok_11,nvl(cast(pok_12 as int),0) pok_12,
		nvl(cast(pok_13 as int),0) pok_13,nvl(cast(pok_14 as int),0) pok_14,nvl(cast(pok_15 as int),0) pok_15,
		nvl(cast(pok_16 as int),0) pok_16,nvl(cast(pok_17 as int),0) pok_17,nvl(cast(pok_18 as int),0) pok_18,
		nvl(cast(pok_19 as int),0) pok_19,nvl(cast(pok_20 as int),0) pok_20,nvl(cast(pok_21 as int),0) pok_21,
		nvl(cast(pok_22 as int),0) pok_22,nvl(cast(pok_23 as int),0) pok_23,
		nvl(cast(pok_24 as float),0) pok_24,nvl(cast(pok_25 as float),0) pok_25,
		nvl(cast(pokg1 as int),0) pokg1,nvl(cast(pokg2 as int),0) pokg2,nvl(cast(pokg3 as int),0) pokg3,
		nvl(cast(pokg4 as int),0) pokg4,nvl(cast(pokg5 as int),0) pokg5,nvl(cast(pokg6 as int),0) pokg6,
		nvl(cast(pok_26 as float),0) pok_26,nvl(cast(pok_27 as float),0) pok_27,
		nvl(cast(pok_28 as int),0) pok_28,
		pok_29
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
WHERE rf.CODE = '18.1 COVID 19' 
and r.BDATE BETWEEN to_date('2021-06-30','yyyy-mm-dd') AND to_date('2021-07-07','yyyy-mm-dd')
and bi.CODE in ('COV_СИЗ_А','COV_СИЗ_В','COV_СИЗ_Г','COV_СИЗ_Д','COV_СИЗ_02_01_4','COV_СИЗ_03_01_4','COV_СИЗ_04_01_4',
				'COV_СИЗ_05_01_4','COV_СИЗ_06_01_4','COV_СИЗ_07_01_4','COV_СИЗ_08_01_4','COV_СИЗ_09_01_4','COV_СИЗ_10_01_4',
				'COV_СИЗ_11_01_4','COV_СИЗ_12_01_4','COV_СИЗ_13_01_4','COV_СИЗ_14_01_4','COV_СИЗ_15_01_4','COV_СИЗ_16_01_4',
				'COV_СИЗ_17_01_4','COV_СИЗ_18_01_4','COV_СИЗ_19_01_4','COV_СИЗ_20_01_4','COV_СИЗ_21_01_4','COV_СИЗ_22_01_4',
				'COV_СИЗ_23_01_4','COV_СИЗ_24_01_4','COV_СИЗ_25_01_4',
				'COV_СИЗ_Г_1_1_4','COV_СИЗ_Г_2_1_4','COV_СИЗ_Г_3_1_4','COV_СИЗ_Г_4_1_4','COV_СИЗ_Г_5_1_4','COV_СИЗ_Г_6_1_4',
				'COV_СИЗ_26_01_4','COV_СИЗ_27_01_4','COV_СИЗ_28_01_4','COV_СИЗ_29_01_4')
order by  d.BALANCEINDEX 
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('COV_СИЗ_А'  disrict,'COV_СИЗ_В' podch,'COV_СИЗ_Г' men, 'COV_СИЗ_Д' tel, 'COV_СИЗ_02_01_4'  pok_02,
				  'COV_СИЗ_03_01_4' pok_03,'COV_СИЗ_04_01_4' pok_04,'COV_СИЗ_05_01_4' pok_05, 'COV_СИЗ_06_01_4'  pok_06,
				  'COV_СИЗ_07_01_4' pok_07, 'COV_СИЗ_08_01_4' pok_08, 'COV_СИЗ_09_01_4' pok_09,'COV_СИЗ_10_01_4' pok_10,
				  'COV_СИЗ_11_01_4' pok_11, 'COV_СИЗ_12_01_4' pok_12, 'COV_СИЗ_13_01_4' pok_13, 'COV_СИЗ_14_01_4' pok_14,
				  'COV_СИЗ_15_01_4' pok_15, 'COV_СИЗ_16_01_4' pok_16, 'COV_СИЗ_17_01_4' pok_17, 'COV_СИЗ_18_01_4' pok_18,
				  'COV_СИЗ_19_01_4' pok_19, 'COV_СИЗ_20_01_4' pok_20, 'COV_СИЗ_21_01_4' pok_21, 'COV_СИЗ_22_01_4' pok_22,
				  'COV_СИЗ_23_01_4' pok_23, 'COV_СИЗ_24_01_4' pok_24, 'COV_СИЗ_25_01_4' pok_25,
				  'COV_СИЗ_Г_1_1_4' pokg1,'COV_СИЗ_Г_2_1_4' pokg2,'COV_СИЗ_Г_3_1_4' pokg3,'COV_СИЗ_Г_4_1_4' pokg4,'COV_СИЗ_Г_5_1_4' pokg5,'COV_СИЗ_Г_6_1_4' pokg6,
				  'COV_СИЗ_26_01_4' pok_26,'COV_СИЗ_27_01_4' pok_27, 'COV_СИЗ_28_01_4' pok_28,'COV_СИЗ_29_01_4' pok_29
				  )
)
UNION ALL
SELECT  day, disrict,  podch, ORGANIZATION,  '2. Наличие СИЗ и дез. растворов в  МО и на складах' as TYPE,
		nvl(cast(pok_02 as int),0) pok_02,nvl(cast(pok_03 as int),0) pok_03,nvl(cast(pok_04 as int),0) pok_04,
		nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,nvl(cast(pok_07 as int),0) pok_07,
		nvl(cast(pok_08 as int),0) pok_08,nvl(cast(pok_09 as int),0) pok_09,nvl(cast(pok_10 as int),0) pok_10,
		nvl(cast(pok_11 as int),0) pok_11,nvl(cast(pok_12 as int),0) pok_12,
		nvl(cast(pok_13 as int),0) pok_13,nvl(cast(pok_14 as int),0) pok_14,nvl(cast(pok_15 as int),0) pok_15,
		nvl(cast(pok_16 as int),0) pok_16,nvl(cast(pok_17 as int),0) pok_17,nvl(cast(pok_18 as int),0) pok_18,
		nvl(cast(pok_19 as int),0) pok_19,nvl(cast(pok_20 as int),0) pok_20,nvl(cast(pok_21 as int),0) pok_21,
		nvl(cast(pok_22 as int),0) pok_22,nvl(cast(pok_23 as int),0) pok_23,
		nvl(cast(pok_24 as float),0) pok_24,nvl(cast(pok_25 as float),0) pok_25,
		nvl(cast(pokg1 as int),0) pokg1,nvl(cast(pokg2 as int),0) pokg2,nvl(cast(pokg3 as int),0) pokg3,
		nvl(cast(pokg4 as int),0) pokg4,nvl(cast(pokg5 as int),0) pokg5,nvl(cast(pokg6 as int),0) pokg6,
		nvl(cast(pok_26 as float),0) pok_26,nvl(cast(pok_27 as float),0) pok_27,
		nvl(cast(pok_28 as int),0) pok_28,
		pok_29
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
WHERE rf.CODE = '18.1 COVID 19' 
and r.BDATE BETWEEN to_date('2021-06-30','yyyy-mm-dd') AND to_date('2021-07-07','yyyy-mm-dd')
and bi.CODE in ('COV_СИЗ_А','COV_СИЗ_В','COV_СИЗ_Г','COV_СИЗ_Д','COV_СИЗ_02_02_4','COV_СИЗ_03_02_4','COV_СИЗ_04_02_4',
				'COV_СИЗ_05_02_4','COV_СИЗ_06_02_4','COV_СИЗ_07_02_4','COV_СИЗ_08_02_4','COV_СИЗ_09_02_4','COV_СИЗ_10_02_4',
				'COV_СИЗ_11_02_4','COV_СИЗ_12_02_4','COV_СИЗ_13_02_4','COV_СИЗ_14_02_4','COV_СИЗ_15_02_4','COV_СИЗ_16_02_4',
				'COV_СИЗ_17_02_4','COV_СИЗ_18_02_4','COV_СИЗ_19_02_4','COV_СИЗ_20_02_4','COV_СИЗ_21_02_4','COV_СИЗ_22_02_4',
				'COV_СИЗ_23_02_4','COV_СИЗ_24_02_4','COV_СИЗ_25_02_4',
				'COV_СИЗ_Г_1_2_4','COV_СИЗ_Г_2_2_4','COV_СИЗ_Г_3_2_4','COV_СИЗ_Г_4_2_4','COV_СИЗ_Г_5_2_4','COV_СИЗ_Г_6_2_4',
				'COV_СИЗ_26_02_4','COV_СИЗ_27_02_4','COV_СИЗ_28_02_4','COV_СИЗ_29_02_4')
order by  d.BALANCEINDEX 
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('COV_СИЗ_А'  disrict,'COV_СИЗ_В' podch,'COV_СИЗ_Г' men, 'COV_СИЗ_Д' tel, 'COV_СИЗ_02_02_4'  pok_02,
				  'COV_СИЗ_03_02_4' pok_03,'COV_СИЗ_04_02_4' pok_04,'COV_СИЗ_05_02_4' pok_05, 'COV_СИЗ_06_02_4'  pok_06,
				  'COV_СИЗ_07_02_4' pok_07, 'COV_СИЗ_08_02_4' pok_08, 'COV_СИЗ_09_02_4' pok_09,'COV_СИЗ_10_02_4' pok_10,
				  'COV_СИЗ_11_02_4' pok_11, 'COV_СИЗ_12_02_4' pok_12, 'COV_СИЗ_13_02_4' pok_13, 'COV_СИЗ_14_02_4' pok_14,
				  'COV_СИЗ_15_02_4' pok_15, 'COV_СИЗ_16_02_4' pok_16, 'COV_СИЗ_17_02_4' pok_17, 'COV_СИЗ_18_02_4' pok_18,
				  'COV_СИЗ_19_02_4' pok_19, 'COV_СИЗ_20_02_4' pok_20, 'COV_СИЗ_21_02_4' pok_21, 'COV_СИЗ_22_02_4' pok_22,
				  'COV_СИЗ_23_02_4' pok_23, 'COV_СИЗ_24_02_4' pok_24, 'COV_СИЗ_25_02_4' pok_25,
				  'COV_СИЗ_Г_1_2_4' pokg1,'COV_СИЗ_Г_2_2_4' pokg2,'COV_СИЗ_Г_3_2_4' pokg3,'COV_СИЗ_Г_4_2_4' pokg4,'COV_СИЗ_Г_5_2_4' pokg5,'COV_СИЗ_Г_6_2_4' pokg6,
				  'COV_СИЗ_26_02_4' pok_26,'COV_СИЗ_27_02_4' pok_27, 'COV_СИЗ_28_02_4' pok_28,'COV_СИЗ_29_02_4' pok_29
				  )
)
UNION ALL 
SELECT  day, disrict,  podch, ORGANIZATION,  '3. Ежедневный расход **' as TYPE,
		nvl(cast(pok_02 as int),0) pok_02,nvl(cast(pok_03 as int),0) pok_03,nvl(cast(pok_04 as int),0) pok_04,
		nvl(cast(pok_05 as int),0) pok_05,nvl(cast(pok_06 as int),0) pok_06,nvl(cast(pok_07 as int),0) pok_07,
		nvl(cast(pok_08 as int),0) pok_08,nvl(cast(pok_09 as int),0) pok_09,nvl(cast(pok_10 as int),0) pok_10,
		nvl(cast(pok_11 as int),0) pok_11,nvl(cast(pok_12 as int),0) pok_12,
		nvl(cast(pok_13 as int),0) pok_13,nvl(cast(pok_14 as int),0) pok_14,nvl(cast(pok_15 as int),0) pok_15,
		nvl(cast(pok_16 as int),0) pok_16,nvl(cast(pok_17 as int),0) pok_17,nvl(cast(pok_18 as int),0) pok_18,
		nvl(cast(pok_19 as int),0) pok_19,nvl(cast(pok_20 as int),0) pok_20,nvl(cast(pok_21 as int),0) pok_21,
		nvl(cast(pok_22 as int),0) pok_22,nvl(cast(pok_23 as int),0) pok_23,
		nvl(cast(pok_24 as float),0) pok_24,nvl(cast(pok_25 as float),0) pok_25,
		nvl(cast(pokg1 as int),0) pokg1,nvl(cast(pokg2 as int),0) pokg2,nvl(cast(pokg3 as int),0) pokg3,
		nvl(cast(pokg4 as int),0) pokg4,nvl(cast(pokg5 as int),0) pokg5,nvl(cast(pokg6 as int),0) pokg6,
		nvl(cast(pok_26 as float),0) pok_26,nvl(cast(pok_27 as float),0) pok_27,
		nvl(cast(pok_28 as int),0) pok_28,
		pok_29
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
WHERE rf.CODE = '18.1 COVID 19' 
and r.BDATE BETWEEN to_date('2021-06-30','yyyy-mm-dd') AND to_date('2021-07-07','yyyy-mm-dd')
and bi.CODE in ('COV_СИЗ_А','COV_СИЗ_В','COV_СИЗ_Г','COV_СИЗ_Д','COV_СИЗ_03_02_4','COV_СИЗ_03_03_4','COV_СИЗ_04_03_4',
				'COV_СИЗ_05_03_4','COV_СИЗ_06_03_4','COV_СИЗ_07_03_4','COV_СИЗ_08_03_4','COV_СИЗ_09_03_4','COV_СИЗ_10_03_4',
				'COV_СИЗ_11_03_4','COV_СИЗ_12_03_4','COV_СИЗ_13_03_4','COV_СИЗ_14_03_4','COV_СИЗ_15_03_4','COV_СИЗ_16_03_4',
				'COV_СИЗ_17_03_4','COV_СИЗ_18_03_4','COV_СИЗ_19_03_4','COV_СИЗ_20_03_4','COV_СИЗ_21_03_4','COV_СИЗ_22_03_4',
				'COV_СИЗ_23_03_4','COV_СИЗ_24_03_4','COV_СИЗ_25_03_4',
				'COV_СИЗ_Г_1_3_4','COV_СИЗ_Г_2_3_4','COV_СИЗ_Г_3_3_4','COV_СИЗ_Г_4_3_4','COV_СИЗ_Г_5_3_4','COV_СИЗ_Г_6_3_4',
				'COV_СИЗ_26_03_4','COV_СИЗ_27_03_4','COV_СИЗ_28_03_4','COV_СИЗ_29_03_4')
order by  d.BALANCEINDEX 
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('COV_СИЗ_А'  disrict,'COV_СИЗ_В' podch,'COV_СИЗ_Г' men, 'COV_СИЗ_Д' tel, 'COV_СИЗ_03_02_4'  pok_02,
				  'COV_СИЗ_03_03_4' pok_03,'COV_СИЗ_04_03_4' pok_04,'COV_СИЗ_05_03_4' pok_05, 'COV_СИЗ_06_03_4'  pok_06,
				  'COV_СИЗ_07_03_4' pok_07, 'COV_СИЗ_08_03_4' pok_08, 'COV_СИЗ_09_03_4' pok_09,'COV_СИЗ_10_03_4' pok_10,
				  'COV_СИЗ_11_03_4' pok_11, 'COV_СИЗ_12_03_4' pok_12, 'COV_СИЗ_13_03_4' pok_13, 'COV_СИЗ_14_03_4' pok_14,
				  'COV_СИЗ_15_03_4' pok_15, 'COV_СИЗ_16_03_4' pok_16, 'COV_СИЗ_17_03_4' pok_17, 'COV_СИЗ_18_03_4' pok_18,
				  'COV_СИЗ_19_03_4' pok_19, 'COV_СИЗ_20_03_4' pok_20, 'COV_СИЗ_21_03_4' pok_21, 'COV_СИЗ_22_03_4' pok_22,
				  'COV_СИЗ_23_03_4' pok_23, 'COV_СИЗ_24_03_4' pok_24, 'COV_СИЗ_25_03_4' pok_25,
				  'COV_СИЗ_Г_1_3_4' pokg1,'COV_СИЗ_Г_2_3_4' pokg2,'COV_СИЗ_Г_3_3_4' pokg3,'COV_СИЗ_Г_4_3_4' pokg4,'COV_СИЗ_Г_5_3_4' pokg5,'COV_СИЗ_Г_6_3_4' pokg6,
				  'COV_СИЗ_26_03_4' pok_26,'COV_СИЗ_27_03_4' pok_27, 'COV_СИЗ_28_03_4' pok_28,'COV_СИЗ_29_03_4' pok_29
				  )
)
UNION ALL 
SELECT  day, disrict,  podch, ORGANIZATION,  '4. СИЗ закупленные и ожидаемые к поставке' as TYPE,
		null as pok_02,nvl(cast(pok_03 as int),0) pok_03,null as pok_04,
		null as  pok_05,nvl(cast(pok_06 as int),0) pok_06,null as pok_07,
		null as pok_08,nvl(cast(pok_09 as int),0) pok_09,nvl(cast(pok_10 as int),0) pok_10,
		null as pok_11,null as pok_12,
		nvl(cast(pok_13 as int),0) pok_13, null as pok_14, null as pok_15,
		nvl(cast(pok_16 as int),0) pok_16, null as pok_17,null as pok_18,
		nvl(cast(pok_19 as int),0) pok_19,null as pok_20,nvl(cast(pok_21 as int),0) pok_21,
		null as pok_22,null as pok_23,null as pok_24,null as pok_25,
		null as pokg1,null as pokg2,null as pokg3,null as pokg4,null as pokg5,null as pokg6,
		null as pok_26,null as pok_27,null as pok_28,pok_29
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
WHERE rf.CODE = '18.1 COVID 19' 
and r.BDATE BETWEEN to_date('2021-06-30','yyyy-mm-dd') AND to_date('2021-07-07','yyyy-mm-dd')
and bi.CODE in ('COV_СИЗ_А','COV_СИЗ_В','COV_СИЗ_Г','COV_СИЗ_Д','COV_СИЗ_04_03_4',
				'COV_СИЗ_06_04_4','COV_СИЗ_09_04_4','COV_СИЗ_10_04_4','COV_СИЗ_13_04_4',
				'COV_СИЗ_16_04_4','COV_СИЗ_19_04_4','COV_СИЗ_21_04_4','COV_СИЗ_29_04_4')
order by  d.BALANCEINDEX 
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('COV_СИЗ_А'  disrict,'COV_СИЗ_В' podch,'COV_СИЗ_Г' men, 'COV_СИЗ_Д' tel, 
				  'COV_СИЗ_04_03_4' pok_03,'COV_СИЗ_06_04_4'  pok_06,
				  'COV_СИЗ_09_04_4' pok_09,'COV_СИЗ_10_04_4' pok_10,
				  'COV_СИЗ_13_04_4' pok_13,'COV_СИЗ_16_04_4' pok_16,
				  'COV_СИЗ_19_04_4' pok_19, 'COV_СИЗ_21_04_4' pok_21,'COV_СИЗ_29_04_4' pok_29
				  )
)
ORDER BY DAY,disrict,organization,TYPE
