SEECT * FROM (
		SELECT
		to_char(r.BDATE, 'DD.MM.YYYY') day,
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
		WHERE rf.code = '50 COVID 19'
		and  r.BDATE =  (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
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
								WHERE rf.code = '50 COVID 19'
								AND r.BDATE < SYSDATE + 2 )
		and bi.CODE in ('50_cov_09','50_cov_10','50_cov_11','50_cov_12','50_cov_13','50_cov_14',
						'50_cov_15','50_cov_16','50_cov_17','50_cov_18','50_cov_19','50_cov_20','50_cov_21'))
pivot
(
		max(value)
		FOR POKAZATEL IN ('50_cov_09' cov_09,'50_cov_10' cov_10,'50_cov_11' cov_11,'50_cov_12' cov_12,'50_cov_13' cov_13,'50_cov_14' cov_14,
'50_cov_15' cov_15,'50_cov_16' cov_16,'50_cov_17' cov_17,'50_cov_18' cov_18,'50_cov_19' cov_19,'50_cov_20' cov_20,'50_cov_21' cov_21
))
WHERE ORGANIZATION IN (
'АНО "Медицинский центр "Двадцать первый век"',
'ООО "Ава-Петер"',
'ООО "Городские поликлиники"',
'ООО "Медицентр ЮЗ"',
'ООО "Современная медицина"',
'ООО "Участковые врачи"',
'ООО "Центр Семейной Медицины "XXI век"',
'СПб ГАУЗ "Городская поликлиника №40"',
'СПб ГБУЗ "Городская больница №40"',
'СПб ГБУЗ "Городская поликлиника №100 Невского района Санкт-Петербурга"',
'СПб ГБУЗ "Городская поликлиника №102"',
'СПб ГБУЗ "Городская поликлиника №104"',
'СПб ГБУЗ "Городская поликлиника №106"',
'СПб ГБУЗ "Городская поликлиника №107"',
'СПб ГБУЗ "Городская поликлиника №109"',
'СПб ГБУЗ "Городская поликлиника №111"',
'СПб ГБУЗ "Городская поликлиника №112"',
'СПб ГБУЗ "Городская поликлиника №114"',
'СПб ГБУЗ "Городская поликлиника №117"',
'СПб ГБУЗ "Городская поликлиника №118"',
'СПб ГБУЗ "Городская поликлиника №120"',
'СПб ГБУЗ "Городская поликлиника №122"',
'СПб ГБУЗ "Городская поликлиника №14"',
'СПб ГБУЗ "Городская поликлиника №17"',
'СПб ГБУЗ "Городская поликлиника №19"',
'СПб ГБУЗ "Городская поликлиника №21"',
'СПб ГБУЗ "Городская поликлиника №22"',
'СПб ГБУЗ "Городская поликлиника №23"',
'СПб ГБУЗ "Городская поликлиника №24"',
'СПб ГБУЗ "Городская поликлиника №25 Невского района"',
'СПб ГБУЗ "Городская поликлиника №27"',
'СПб ГБУЗ "Городская поликлиника №28"',
'СПб ГБУЗ "Городская поликлиника №3"',
'СПб ГБУЗ "Городская поликлиника №30"',
'СПб ГБУЗ "Городская поликлиника №32"',
'СПб ГБУЗ "Городская поликлиника №34"',
'СПб ГБУЗ "Городская поликлиника №37"',
'СПб ГБУЗ "Городская поликлиника №38"',
'СПб ГБУЗ "Городская поликлиника №39"',
'СПб ГБУЗ "Городская поликлиника №4"',
'СПб ГБУЗ "Городская поликлиника №43"',
'СПб ГБУЗ "Городская поликлиника №44"',
'СПб ГБУЗ "Городская поликлиника №46"',
'СПб ГБУЗ "Городская поликлиника №48"',
'СПб ГБУЗ "Городская поликлиника №49"',
'СПб ГБУЗ "Городская поликлиника №51"',
'СПб ГБУЗ "Городская поликлиника №52"',
'СПб ГБУЗ "Городская поликлиника №54"',
'СПб ГБУЗ "Городская поликлиника №56"',
'СПб ГБУЗ "Городская поликлиника №6"',
'СПб ГБУЗ "Городская поликлиника №60 Пушкинского района"',
'СПб ГБУЗ "Городская поликлиника №63"',
'СПб ГБУЗ "Городская поликлиника №71"',
'СПб ГБУЗ "Городская поликлиника №72"',
'СПб ГБУЗ "Городская поликлиника №74"',
'СПб ГБУЗ "Городская поликлиника №76"',
'СПб ГБУЗ "Городская поликлиника №77 Невского района"',
'СПб ГБУЗ "Городская поликлиника №78"',
'СПб ГБУЗ "Городская поликлиника №8"',
'СПб ГБУЗ "Городская поликлиника №86"',
'СПб ГБУЗ "Городская поликлиника №87"',
'СПб ГБУЗ "Городская поликлиника №88"',
'СПб ГБУЗ "Городская поликлиника №91"',
'СПб ГБУЗ "Городская поликлиника №93"',
'СПб ГБУЗ "Городская поликлиника №94"',
'СПб ГБУЗ "Городская поликлиника №95"',
'СПб ГБУЗ "Городская поликлиника №96"',
'СПб ГБУЗ "Городская поликлиника №97"',
'СПб ГБУЗ "Городская поликлиника №98"',
'СПб ГБУЗ "Городская поликлиника №99"',
'СПб ГБУЗ "Городской противотуберкулезный диспансер"',
'СПб ГБУЗ "Детская городская поликлиника №11"',
'СПб ГБУЗ "Детская городская поликлиника №17"',
'СПб ГБУЗ "Детская городская поликлиника №19"',
'СПб ГБУЗ "Детская городская поликлиника №29"',
'СПб ГБУЗ "Детская городская поликлиника №35"',
'СПб ГБУЗ "Детская городская поликлиника №44"',
'СПб ГБУЗ "Детская городская поликлиника №45 Невского района"',
'СПб ГБУЗ "Детская городская поликлиника №49" Пушкинского района',
'СПб ГБУЗ "Детская городская поликлиника №51"',
'СПб ГБУЗ "Детская городская поликлиника №62"',
'СПб ГБУЗ "Детская городская поликлиника №68"',
'СПб ГБУЗ "Детская городская поликлиника №7"',
'СПб ГБУЗ "Детская городская поликлиника №71"',
'СПб ГБУЗ "Детская городская поликлиника №73"',
'СПб ГБУЗ "Детская городская поликлиника №8"',
'СПб ГБУЗ ДП № 30',
'СПб ГБУЗ "Николаевская больница"',
'СПб ГМУ им.И.П.Павлова "Городская поликлиника №31"',
'ФГБОУ ВО СЗГМУ им. И.И. МЕЧНИКОВА МИНЗДРАВА РОССИИ',
'ФГБУ "СЗОНКЦ им.Л.Г.Соколова ФМБА России',
'ЧУЗ «КБ «РЖД-МЕДИЦИНА» Г. С-ПЕТЕРБУРГ»'
)
ORDER BY ORGANIZATION
