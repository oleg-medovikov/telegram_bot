SELECT  ORGANIZATION, 'Пункт вакцинации' type, 
                substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,
                TRIM(LEADING ' ' from REPLACE(substr(tvsp ,INSTR(tvsp , ' ')+1, length(tvsp)),'район ','') ) tvsp, 
                CAST(Vaccin_tvsp_03 AS int) Vaccin_tvsp_03 , CAST(Vaccin_tvsp_04 AS int) Vaccin_tvsp_04, 
                CAST(Vaccin_tvsp_04_day AS int) Vaccin_tvsp_04_day, CAST(Vaccin_tvsp_05 AS int) Vaccin_tvsp_05,
                CAST(Vaccin_tvsp_06 AS int) Vaccin_tvsp_06, CAST(Vaccin_tvsp_07 AS int) Vaccin_tvsp_07, CAST(Vaccin_tvsp_08 AS int) Vaccin_tvsp_08,
                CAST(Vaccin_tvsp_09 AS int) Vaccin_tvsp_09, CAST(Vaccin_tvsp_10 AS int) Vaccin_tvsp_10,
                CAST(Vaccin_tvsp_11 AS int) Vaccin_tvsp_11, CAST(Vaccin_tvsp_12 AS int) Vaccin_tvsp_12, 
                CAST(Vaccin_tvsp_20 AS int) Vaccin_tvsp_20, CAST(Vaccin_tvsp_20_day AS int) Vaccin_tvsp_20_day,
                CAST(Vaccin_tvsp_13 AS int) Vaccin_tvsp_13, CAST(Vaccin_tvsp_14 AS int) Vaccin_tvsp_14, CAST(Vaccin_tvsp_15 AS int) Vaccin_tvsp_15,
                CAST(Vaccin_tvsp_16 AS int) Vaccin_tvsp_16, CAST(Vaccin_tvsp_17 AS int) Vaccin_tvsp_17,
                CAST(Vaccin_tvsp_18 AS int) Vaccin_tvsp_18, CAST(Vaccin_tvsp_19 AS int) Vaccin_tvsp_19, CAST(Vaccin_tvsp_19_day AS int) Vaccin_tvsp_19_day,
                CAST(Vaccin_tvsp_21 AS int) Vaccin_tvsp_21, CAST(Vaccin_tvsp_21_day AS int) Vaccin_tvsp_21_day,
                CAST(revac_20_01 AS int) revac_20_01 
        FROM (
                SELECT
                        r.BDATE day,
                        a.AGNNAME organization,
                    i.CODE pokazatel,
                    ro.NUMB row_index ,
                        CASE WHEN STRVAL  IS NOT NULL THEN STRVAL
                                 WHEN NUMVAL  IS NOT NULL THEN CAST(NUMVAL  AS varchar(30))
                                 WHEN DATEVAL IS NOT NULL THEN CAST(DATEVAL AS varchar(30))
                                ELSE NULL END value
                FROM PARUS.BLTBLVALUES v
                INNER JOIN PARUS.BLTABLESIND si
                on(v.BLTABLESIND = si.RN)
                INNER JOIN PARUS.BALANCEINDEXES i
                on(si.BALANCEINDEXES = i.RN)
                INNER JOIN PARUS.BLTBLROWS ro
                on(v.PRN = ro.RN)
                INNER JOIN PARUS.BLSUBREPORTS s
                on(ro.PRN = s.RN)
                INNER JOIN PARUS.BLREPORTS r
                on(s.PRN = r.RN)
                INNER JOIN PARUS.AGNLIST a
                on(r.AGENT = a.RN)
                INNER JOIN PARUS.BLREPFORMED rd
                on(r.BLREPFORMED = rd.RN)
                INNER JOIN PARUS.BLREPFORM rf
                on(rd.PRN = rf.RN)
                WHERE rf.code = '40 COVID 19'
                and r.BDATE =  trunc(SYSDATE) - 2 
                and ro.BLTABLES = (SELECT BLTABLES FROM (
 								SELECT DISTINCT ro.BLTABLES , ROW_NUMBER () over(ORDER BY ro.BLTABLES desc) AS num
					                FROM PARUS.BLTBLVALUES v
					                INNER JOIN PARUS.BLTABLESIND si
					                on(v.BLTABLESIND = si.RN)
					                INNER JOIN PARUS.BALANCEINDEXES i
					                on(si.BALANCEINDEXES = i.RN)
					                INNER JOIN PARUS.BLTBLROWS ro
					                on(v.PRN = ro.RN)
					                INNER JOIN PARUS.BLSUBREPORTS s
					                on(ro.PRN = s.RN)
					                INNER JOIN PARUS.BLREPORTS r
					                on(s.PRN = r.RN)
					                WHERE  r.BDATE =  trunc(SYSDATE) - 2 
					                and i.CODE in ('Vaccin_tvsp_05') 
										) WHERE num = 1)
                and i.CODE in ('Vaccin_TVSP','Vaccin_tvsp_03','Vaccin_tvsp_04','Vaccin_tvsp_04_day','Vaccin_tvsp_05',
                                                'Vaccin_tvsp_06','Vaccin_tvsp_07','Vaccin_tvsp_08', 'Vaccin_tvsp_09', 'Vaccin_tvsp_10',
                                                'Vaccin_tvsp_11', 'Vaccin_tvsp_12', 'Vaccin_tvsp_20','Vaccin_tvsp_20_day',
                                                'Vaccin_tvsp_13','Vaccin_tvsp_14','Vaccin_tvsp_15', 'Vaccin_tvsp_16', 'Vaccin_tvsp_17',
                                                'Vaccin_tvsp_18', 'Vaccin_tvsp_19', 'Vaccin_tvsp_19_day','Vaccin_tvsp_21', 'Vaccin_tvsp_21_day'
                                                , 'revac_20_01')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_tvsp_03' Vaccin_tvsp_03,'Vaccin_tvsp_04' Vaccin_tvsp_04,'Vaccin_tvsp_04_day' Vaccin_tvsp_04_day,
                                                'Vaccin_tvsp_05' Vaccin_tvsp_05, 'Vaccin_tvsp_06' Vaccin_tvsp_06,'Vaccin_tvsp_07' Vaccin_tvsp_07,
                                                'Vaccin_tvsp_08' Vaccin_tvsp_08, 'Vaccin_tvsp_09' Vaccin_tvsp_09, 'Vaccin_tvsp_10' Vaccin_tvsp_10,
                                                'Vaccin_tvsp_11' Vaccin_tvsp_11, 'Vaccin_tvsp_12' Vaccin_tvsp_12, 'Vaccin_tvsp_20' Vaccin_tvsp_20,
                                                'Vaccin_tvsp_20_day' Vaccin_tvsp_20_day,'Vaccin_tvsp_13' Vaccin_tvsp_13,'Vaccin_tvsp_14' Vaccin_tvsp_14,
                                                'Vaccin_tvsp_15' Vaccin_tvsp_15, 'Vaccin_tvsp_16' Vaccin_tvsp_16, 'Vaccin_tvsp_17' Vaccin_tvsp_17,
                                                'Vaccin_tvsp_18' Vaccin_tvsp_18, 'Vaccin_tvsp_19' Vaccin_tvsp_19, 'Vaccin_tvsp_19_day' Vaccin_tvsp_19_day,
                                                'Vaccin_tvsp_21' Vaccin_tvsp_21, 'Vaccin_tvsp_21_day' Vaccin_tvsp_21_day, 'revac_20_01' revac_20_01)
                )
                WHERE tvsp IS NOt NULL
                and ORGANIZATION NOT LIKE 'Администр%'
        UNION ALL
                SELECT    			ORGANIZATION,	'Медицинская организация' type, 
                                REPLACE(dist,' район ','') dist,
                                case when tvsp is null then organization else tvsp end tvsp, 
                                CAST(Vaccin_03 AS int) Vaccin_03 , CAST(Vaccin_04 AS int) Vaccin_04, 
                                CAST(Vaccin_04_day AS int) Vaccin_04_day, CAST(Vaccin_05 AS int) Vaccin_05,
                                CAST(Vaccin_06 AS int) Vaccin_06, CAST(Vaccin_07 AS int) Vaccin_07, CAST(Vaccin_08 AS int) Vaccin_08,
                                CAST(Vaccin_09 AS int) Vaccin_09, CAST(Vaccin_10 AS int) Vaccin_10,
                                CAST(Vaccin_11 AS int) Vaccin_11, CAST(Vaccin_12 AS int) Vaccin_12, 
                                CAST(Vaccin_20 AS int) Vaccin_20, CAST(Vaccin_20_day AS int) Vaccin_20_day,
                                CAST(Vaccin_13 AS int) Vaccin_13, CAST(Vaccin_14 AS int) Vaccin_14, CAST(Vaccin_15 AS int) Vaccin_15,
                                CAST(Vaccin_16 AS int) Vaccin_16, CAST(Vaccin_17 AS int) Vaccin_17,
                                CAST(Vaccin_18 AS int) Vaccin_18, CAST(Vaccin_19 AS int) Vaccin_19, CAST(Vaccin_19_day AS int) Vaccin_19_day,
                                CAST(Vaccin_21 AS int) Vaccin_21, CAST(Vaccin_21_day AS int) Vaccin_21_day,
                                CAST(revac_20_02 AS int) revac_20_02 
                FROM (
                SELECT
                        r.BDATE,
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
                and bi.CODE in ('Vaccin_TVSP','Vaccin_DISTR','Vaccin_03','Vaccin_04','Vaccin_04_day','Vaccin_05',
                                                'Vaccin_06','Vaccin_07','Vaccin_08', 'Vaccin_09', 'Vaccin_10',
                                                'Vaccin_11', 'Vaccin_12', 'Vaccin_20','Vaccin_20_day',
                                                'Vaccin_13','Vaccin_14','Vaccin_15', 'Vaccin_16', 'Vaccin_17',
                                                'Vaccin_18', 'Vaccin_19', 'Vaccin_19_day','Vaccin_21', 'Vaccin_21_day'
                                                , 'revac_20_02_s')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_DISTR' dist,'Vaccin_03' Vaccin_03,'Vaccin_04' Vaccin_04,'Vaccin_04_day' Vaccin_04_day,
                                                'Vaccin_05' Vaccin_05, 'Vaccin_06' Vaccin_06,'Vaccin_07' Vaccin_07,
                                                'Vaccin_08' Vaccin_08, 'Vaccin_09' Vaccin_09, 'Vaccin_10' Vaccin_10,
                                                'Vaccin_11' Vaccin_11, 'Vaccin_12' Vaccin_12, 'Vaccin_20' Vaccin_20,
                                                'Vaccin_20_day' Vaccin_20_day,'Vaccin_13' Vaccin_13,'Vaccin_14' Vaccin_14,
                                                'Vaccin_15' Vaccin_15, 'Vaccin_16' Vaccin_16, 'Vaccin_17' Vaccin_17,
                                                'Vaccin_18' Vaccin_18, 'Vaccin_19' Vaccin_19, 'Vaccin_19_day' Vaccin_19_day,
                                                'Vaccin_21' Vaccin_21, 'Vaccin_21_day' Vaccin_21_day, 'revac_20_02_s' revac_20_02)
                )
        WHERE ORGANIZATION NOT LIKE 'Администр%'
        ORDER BY ORGANIZATION,TYPE
