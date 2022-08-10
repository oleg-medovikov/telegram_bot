SELECT ORGANIZATION, 'Пункт вакцинации' type,  substr(Vaccin_TVSP ,1,INSTR(Vaccin_TVSP , ' ')-1) dist, 
        REPLACE(substr(Vaccin_TVSP ,INSTR(Vaccin_TVSP , ' ')+1, length(Vaccin_TVSP)),'район ','') Vaccin_TVSP,
        nvl(cast(light_03 as int),0)  light_03,
        nvl(cast(light_04 as int),0)  light_04,nvl(cast(light_05 as int),0)  light_05,
        nvl(cast(light_06 as int),0)  light_06,nvl(cast(light_07 as int),0)  light_07,
        nvl(cast(light_08 as int),0)  light_08,nvl(cast(light_09 as int),0)  light_09,
        nvl(cast(light_10 as int),0)  light_10,nvl(cast(light_11 as int),0)  light_11,
        nvl(cast(light_12 as int),0)  light_12,nvl(cast(light_13 as int),0)  light_13,
        nvl(cast(light_14 as int),0)  light_14,nvl(cast(light_15 as int),0)  light_15,
        nvl(cast(light_16 as int),0)  light_16,nvl(cast(light_17 as int),0)  light_17,
        nvl(cast(light_18 as int),0)  light_18,nvl(cast(light_19 as int),0)  light_19,
        nvl(cast(light_20 as int),0)  light_20,nvl(cast(light_21 as int),0)  light_21,
        nvl(cast(light_24 as int),0)  light_24, nvl(cast(light_26 as int),0) light_26,
        nvl(cast(revac_20_01 as int),0)  revac_20_01
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
 								SELECT DISTINCT ro.BLTABLES , ROW_NUMBER () over(ORDER BY ro.BLTABLES DESC) AS num
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
					                and i.CODE in ('light_09') 
										) WHERE num = 1)
                 and i.CODE in ('Vaccin_TVSP','light_03','light_04',
                                                'light_05','light_06', 'light_07', 
                                                'light_08', 'light_09', 'light_10',
                                                'light_11','light_12', 'light_13', 
                                                'light_14', 'light_15','light_16', 'light_17'
                                                , 'light_18', 'light_19', 'light_20', 'light_21'
                                                , 'light_24', 'light_26', 'revac_20_01')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('Vaccin_TVSP'  Vaccin_TVSP,
        'light_03'  light_03,
        'light_04'  light_04,'light_05'  light_05,
        'light_06'  light_06,'light_07'  light_07,
        'light_08'  light_08,'light_09'  light_09,
        'light_10'  light_10,'light_11'  light_11,
        'light_12'  light_12,'light_13'  light_13,
        'light_14'  light_14,'light_15'  light_15,
        'light_16'  light_16,'light_17'  light_17,
        'light_18'  light_18,'light_19'  light_19,
        'light_20'  light_20,'light_21'  light_21,
        'light_24'  light_24,'light_26'  light_26,
        'revac_20_01' revac_20_01
        )
                )
      WHERE Vaccin_TVSP IS NOT null
        UNION
SELECT ORGANIZATION, 'Медицинская организация' TYPE, REPLACE (Vaccin_MO,' район ','') dist,
		Vaccin_MO Vaccin_TVSP, 
        nvl(cast(light_03 as int),0)  light_03,
        nvl(cast(light_04 as int),0)  light_04,nvl(cast(light_05 as int),0)  light_05,
        nvl(cast(light_06 as int),0)  light_06,nvl(cast(light_07 as int),0)  light_07,
        nvl(cast(light_08 as int),0)  light_08,nvl(cast(light_09 as int),0)  light_09,
        nvl(cast(light_10 as int),0)  light_10,nvl(cast(light_11 as int),0)  light_11,
        nvl(cast(light_12 as int),0)  light_12,nvl(cast(light_13 as int),0)  light_13,
        nvl(cast(light_14 as int),0)  light_14,nvl(cast(light_15 as int),0)  light_15,
        nvl(cast(light_16 as int),0)  light_16,nvl(cast(light_17 as int),0)  light_17,
        nvl(cast(light_18 as int),0)  light_18,nvl(cast(light_19 as int),0)  light_19,
        nvl(cast(light_20 as int),0)  light_20,nvl(cast(light_21 as int),0)  light_21,
        nvl(cast(light_24 as int),0)  light_24,nvl(cast(light_26 as int),0)  light_26,
        nvl(cast(revac_20_05 as int),0)  revac_20_05
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
                 and bi.CODE in ('Vaccin_MO','light_03_s','light_04_s','light_05_s',
                                                'light_06_s','light_07_s', 'light_08_s', 
                                                'light_09_s', 'light_10_s', 'light_11_s',
                                                'light_12_s','light_13_s', 'light_14_s', 
                                                'light_15_s', 'light_16_s','light_17_s', 'light_18_s',
                                                'light_19_s', 'light_20_s', 'light_21_s',
                                                'light_24_s', 'light_26_s', 'revac_20_05_s')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('Vaccin_MO'  Vaccin_MO,'light_03_s'  light_03,
        'light_04_s'  light_04,'light_05_s'  light_05,'light_06_s'  light_06,
        'light_07_s'  light_07,'light_08_s'  light_08,'light_09_s'  light_09,
        'light_10_s'  light_10,'light_11_s'  light_11,'light_12_s'  light_12,
        'light_13_s'  light_13,'light_14_s'  light_14,'light_15_s'  light_15,
        'light_16_s'  light_16,'light_17_s'  light_17,'light_18_s'  light_18,
        'light_19_s'  light_19,'light_20_s'  light_20, 'light_21_s'  light_21,
        'light_24_s'  light_24,'light_26_s'  light_26,'revac_20_05_s'  revac_20_05
        )
                ) 
ORDER BY ORGANIZATION,TYPE
