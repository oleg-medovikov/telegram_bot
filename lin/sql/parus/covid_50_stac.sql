SELECT DAY,ORGANIZATION,
	CAST(cov_13 as int) cov_13,CAST(cov_14 as int) cov_14,
	CAST(cov_15 as int) cov_15,CAST(cov_16 as int) cov_16,
	CAST(cov_17 as int) cov_17,CAST(cov_18 as int) cov_18,
	CAST(cov_19 as int) cov_19,CAST(cov_20 as int) cov_20,
	CAST(cov_21 as int) cov_21,CAST(cov_22 as int) cov_22,
	CAST(cov_23 as int) cov_23,CAST(cov_24 as int) cov_24
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
		and bi.CODE in ('50_cov_stac_13','50_cov_stac_14','50_cov_stac_15','50_cov_stac_16','50_cov_stac_17','50_cov_stac_18','50_cov_stac_19','50_cov_stac_20','50_cov_stac_21','50_cov_stac_22','50_cov_stac_23','50_cov_stac_24'))
pivot
(
max(value)
FOR POKAZATEL IN ('50_cov_stac_13' cov_13,'50_cov_stac_14' cov_14,'50_cov_stac_15' cov_15,'50_cov_stac_16' cov_16
		,'50_cov_stac_17' cov_17,'50_cov_stac_18' cov_18,'50_cov_stac_19' cov_19
		,'50_cov_stac_20' cov_20,'50_cov_stac_21' cov_21,'50_cov_stac_22' cov_22
		,'50_cov_stac_23' cov_23,'50_cov_stac_24'
))
WHERE ORGANIZATION IN (
'ВОЕННО-МЕДИЦИНСКАЯ АКАДЕМИЯ ИМЕНИ С.М.КИРОВА',
'ГБУ СПб НИИ СП им. И.И. Джанелидзе',
'СПб ГБУЗ "Александровская больница"',
'СПБ ГБУЗ "Введенская больница"',
'СПб ГБУЗ "Городская больница Святого Великомученика Георгия"',
'СПб ГБУЗ "Городская больница Святого Праведного Иоанна Кронштадтского"',
'СПб ГБУЗ "Городская больница Святой преподобномученицы Елизаветы"',
'СПб ГБУЗ "Городская больница №14"',
'СПб ГБУЗ "Городская больница №15"',
'СПб ГБУЗ "Городская больница №20"',
'СПб ГБУЗ "Городская больница №26"',
'СПб ГБУЗ "Городская больница №28 "Максимилиановская"',
'СПб ГБУЗ "Городская больница №33"',
'СПб ГБУЗ "Городская больница №38 им. Н.А.Семашко"',
'СПб ГБУЗ "Городская больница №40"',
'СПб ГБУЗ "Городская больница №9"',
'СПб ГБУЗ "Городская Мариинская больница"',
'СПб ГБУЗ "Городская многопрофильная больница №2"',
'СПб ГБУЗ "Городская наркологическая больница"',
'СПб ГБУЗ "Городская Покровская больница"',
'СПб ГБУЗ "Городская туберкулезная больница №2"',
'СПб ГБУЗ "Городской гериатрический медико-социальный центр"',
'СПб ГБУЗ "Городской клинический онкологический диспансер"',
'СПб ГБУЗ "Городской противотуберкулезный диспансер"',
'СПб ГБУЗ "Госпиталь для ветеранов войн"',
'СПб ГБУЗ "ГПЦ №1"',
'СПб ГБУЗ "ДГБ №2 святой Марии Магдалины"',
'СПБ ГБУЗ "ДГМКЦ ВМТ им. К.А. Раухфуса"',
'СПб ГБУЗ "Детская городская больница Святой Ольги"',
'СПб ГБУЗ "Детская городская больница №22"',
'СПб ГБУЗ "Детская городская клиническая больница №5 имени Нила Федоровича Филатова"',
'СПб ГБУЗ "Детская инфекционная больница №3"',
'СПБ ГБУЗ "Детский городской многопрофильный клинический специализированный центр высоких медицинских технологий"',
'СПб ГБУЗ "Клиническая больница Святителя Луки"',
'СПб ГБУЗ "Клиническая инфекционная больница им. С.П. Боткина"',
'СПб ГБУЗ "Клиническая ревматологическая больница №25"',
'СПб ГБУЗ "Николаевская больница"',
'СПб ГБУЗ "Психиатрическая больница №1 им.П.П.Кащенко"',
'СПб ГБУЗ "Пушкинский противотуберкулезный диспансер"',
'СПб ГБУЗ "Родильный дом №1 (специализированный)"',
'СПб ГБУЗ "Родильный дом №10"',
'СПб ГБУЗ "Родильный дом №13"',
'СПб ГБУЗ "Родильный дом №16"',
'СПб ГБУЗ "Родильный дом №17"',
'СПб ГБУЗ "Родильный дом №9"',
'СПб ГБУЗ "Центр по профилактике и борьбе со СПИД и инфекционными заболеваниями"',
'СПб ГКУЗ "Городская психиатрическая больница №3 им.И.И.Скворцова-Степанова"',
'СПб ГКУЗ "Городская психиатрическая больница №6 (стационар с диспансером)"',
'СПб ГКУЗ "Психиатрическая больница Святого Николая Чудотворца"',
'ФГБОУ ВО ПСПБГМУ им. И.П. Павлова Минздрава России',
'ФГБОУ ВО СЗГМУ им. И.И. МЕЧНИКОВА МИНЗДРАВА РОССИИ',
'ФГБОУ ВО СПБГПМУ МИНЗДРАВА РОССИИ',
'ФГБУ "НМИЦ ИМ. В.А. АЛМАЗОВА" МИНЗДРАВА РОССИИ',
'ФГБУ "Санкт-Петербургский научно-исследовательский институт фтизиопульмонологии" Минздрава России',
'ФГБУ "СЗОНКЦ им.Л.Г.Соколова ФМБА России',
'ЧУЗ «КБ «РЖД-МЕДИЦИНА» Г. С-ПЕТЕРБУРГ»'
)
ORDER BY ORGANIZATION
