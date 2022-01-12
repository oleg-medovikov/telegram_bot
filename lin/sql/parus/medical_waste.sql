SELECT * 
from(
SELECT 
    to_char(r.BDATE, 'YYYY') year,
    a.AGNNAME ORGANIZATION ,
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
WHERE rf.CODE = 'Отчет по отходам'
AND bi.CODE IN ('s_001','tel01','yesno_1','s_002','tel02','yesno_2','yesno_3',
				'yesno_4','yesno_5','yesno_6','yesno_7','yesno_8','1_01_001',
				'1_01_002','1_01_003','1_01_004','1_01_005','1_01_006','1_01_007',
				'1_01_008','1_01_009','1_01_010','1_01_011','1_01_012','1_01_013',
				'1_01_014','yesno_9','yesno_10','yesno_11','yesno_12','yesno_13',
				'yesno_14','yesno_15','yesno_16','1_01_015','s_003','s_004',
				'yesno_17','yesno_18','yesno_19','s_005','s_006','s_007',
				'yesno_20','s_008','s_009','s_010','s_011','yesno_21','s_012',
				's_013','s_014','s_015','yesno_22','s_016','s_017','s_018',
				's_019','yesno_23','s_020','s_021','s_022','s_023',
				'othodi1','othodi2','othodi3','othodi4','othodi5','othodi6',
				'othodi7','othodi8','othodi9','s_024','s_025','s_026','s_027' )
AND to_char(r.BDATE, 'YYYY') > 2020
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('s_001','tel01','yesno_1','s_002','tel02','yesno_2','yesno_3',
				'yesno_4','yesno_5','yesno_6','yesno_7','yesno_8','1_01_001',
				'1_01_002','1_01_003','1_01_004','1_01_005','1_01_006','1_01_007',
				'1_01_008','1_01_009','1_01_010','1_01_011','1_01_012','1_01_013',
				'1_01_014','yesno_9','yesno_10','yesno_11','yesno_12','yesno_13',
				'yesno_14','yesno_15','yesno_16','1_01_015','s_003','s_004',
				'yesno_17','yesno_18','yesno_19','s_005','s_006','s_007',
				'yesno_20','s_008','s_009','s_010','s_011','yesno_21','s_012',
				's_013','s_014','s_015','yesno_22','s_016','s_017','s_018',
				's_019','yesno_23','s_020','s_021','s_022','s_023',
				'othodi1','othodi2','othodi3','othodi4','othodi5','othodi6',
				'othodi7','othodi8','othodi9','s_024','s_025','s_026','s_027'
                    )
)
ORDER BY ORGANIZATION, YEAR DESC
