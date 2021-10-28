SELECT ORGANIZATION, 'Пункт вакцинации' type,  substr(KV_TVSP_02 ,1,INSTR(KV_TVSP_02 , ' ')-1) dist, 
        REPLACE(substr(KV_TVSP_02 ,INSTR(KV_TVSP_02 , ' ')+1, length(KV_TVSP_02)),'район ','') KV_TVSP_02,
        nvl(cast(KV_TVSP_04 as int),0)  KV_TVSP_04,
        nvl(cast(KV_TVSP_05_z as int),0)  KV_TVSP_05_z,nvl(cast(KV_TVSP_06 as int),0)  KV_TVSP_06,
        nvl(cast(KV_TVSP_07_z as int),0)  KV_TVSP_07_z,nvl(cast(KV_TVSP_08 as int),0)  KV_TVSP_08,
        nvl(cast(KV_TVSP_09_z as int),0)  KV_TVSP_09_z,nvl(cast(KV_TVSP_10 as int),0)  KV_TVSP_10,
        nvl(cast(KV_TVSP_11_z as int),0)  KV_TVSP_11_z,nvl(cast(KV_TVSP_12 as int),0)  KV_TVSP_12,
        nvl(cast(KV_TVSP_13_z as int),0)  KV_TVSP_13_z,nvl(cast(KV_TVSP_14 as int),0)  KV_TVSP_14,
        nvl(cast(KV_TVSP_15_z as int),0)  KV_TVSP_15_z,nvl(cast(KV_TVSP_16 as int),0)  KV_TVSP_16,
        nvl(cast(KV_TVSP_17_z as int),0)  KV_TVSP_17_z,nvl(cast(KV_TVSP_18 as int),0)  KV_TVSP_18,
        nvl(cast(KV_TVSP_19_z as int),0)  KV_TVSP_19_z,
        nvl(cast(KV_TVSP_20 as int),0)  KV_TVSP_20,nvl(cast(KV_TVSP_21 as int),0)  KV_TVSP_21,
        nvl(cast(KV_TVSP_22 as int),0)  KV_TVSP_22,nvl(cast(KV_TVSP_23_z as int),0)  KV_TVSP_23_z, 
        nvl(cast(KV_TVSP_24 as int),0)  KV_TVSP_24,nvl(cast(KV_TVSP_25 as int),0)  KV_TVSP_25,
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
					                and i.CODE in ('KV_TVSP_06') 
										) WHERE num = 1)
                 and i.CODE in ('KV_TVSP_02','KV_TVSP_04','KV_TVSP_05_z',
                                                'KV_TVSP_06','KV_TVSP_07_z', 'KV_TVSP_08', 
                                                'KV_TVSP_09_z', 'KV_TVSP_10', 'KV_TVSP_11_z',
                                                'KV_TVSP_12','KV_TVSP_13_z', 'KV_TVSP_14', 
                                                'KV_TVSP_15_z', 'KV_TVSP_16','KV_TVSP_17_z', 'KV_TVSP_18'
                                                , 'KV_TVSP_19_z', 'KV_TVSP_20', 'KV_TVSP_21', 'KV_TVSP_22'
                                                , 'KV_TVSP_23_z', 'KV_TVSP_24', 'KV_TVSP_25_z', 'revac_20_01')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('KV_TVSP_02'  KV_TVSP_02,
        'KV_TVSP_04'  KV_TVSP_04,
        'KV_TVSP_05_z'  KV_TVSP_05_z,'KV_TVSP_06'  KV_TVSP_06,
        'KV_TVSP_07_z'  KV_TVSP_07_z,
        'KV_TVSP_08'  KV_TVSP_08,
        'KV_TVSP_09_z'  KV_TVSP_09_z,'KV_TVSP_10'  KV_TVSP_10,
        'KV_TVSP_11_z'  KV_TVSP_11_z,
        'KV_TVSP_12'  KV_TVSP_12,
        'KV_TVSP_13_z'  KV_TVSP_13_z,'KV_TVSP_14'  KV_TVSP_14,
        'KV_TVSP_15_z'  KV_TVSP_15_z,
        'KV_TVSP_16'  KV_TVSP_16,
        'KV_TVSP_17_z'  KV_TVSP_17_z,'KV_TVSP_18'  KV_TVSP_18,
        'KV_TVSP_19_z'  KV_TVSP_19_z,
        'KV_TVSP_20'  KV_TVSP_20,'KV_TVSP_21'  KV_TVSP_21,
        'KV_TVSP_22'  KV_TVSP_22, 'KV_TVSP_23_z'  KV_TVSP_23_z,
        'KV_TVSP_24'  KV_TVSP_24,'KV_TVSP_25_z'  KV_TVSP_25,'revac_20_01'  revac_20_01
        )
                )
        UNION
        SELECT ORGANIZATION, 'Медицинская организация' TYPE, REPLACE (epy_vak_01,' район ','') dist, epy_vak_02,
        nvl(cast(epy_vak_04 as int),0)  epy_vak_04,nvl(cast(epy_vak_05_z as int),0)  epy_vak_05_z,
        nvl(cast(epy_vak_06 as int),0)  epy_vak_06,nvl(cast(epy_vak_07_z as int),0)  epy_vak_07_z,
        nvl(cast(epy_vak_08 as int),0)  epy_vak_08,nvl(cast(epy_vak_09_z as int),0)  epy_vak_09_z,
        nvl(cast(epy_vak_10 as int),0)  epy_vak_10,nvl(cast(epy_vak_11_z as int),0)  epy_vak_11_z,
        nvl(cast(epy_vak_12 as int),0)  epy_vak_12,nvl(cast(epy_vak_13_z as int),0)  epy_vak_13_z,
        nvl(cast(epy_vak_14 as int),0)  epy_vak_14,nvl(cast(epy_vak_15_z as int),0)  epy_vak_15_z,
        nvl(cast(epy_vak_16 as int),0)  epy_vak_16,nvl(cast(epy_vak_17_z as int),0)  epy_vak_17_z,
        nvl(cast(epy_vak_18 as int),0)  epy_vak_18,nvl(cast(epy_vak_19_z as int),0)  epy_vak_19_z,
        nvl(cast(epy_vak_20 as int),0)  epy_vak_20,nvl(cast(epy_vak_21   as int),0)  epy_vak_21,
        nvl(cast(epy_vak_22 as int),0)  epy_vak_22,nvl(cast(epy_vak_23_z as int),0)  epy_vak_23_z,
        nvl(cast(epy_vak_24 as int),0)  epy_vak_24,nvl(cast(epy_vak_25 as int),0)  epy_vak_25,
        nvl(cast(revac_20_03 as int),0)  revac_20_03
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
                 and bi.CODE in ('epy_vak_01','epy_vak_02','epy_vak_04','epy_vak_05_z',
                                                'epy_vak_06','epy_vak_07_z', 'epy_vak_08', 
                                                'epy_vak_09_z', 'epy_vak_10', 'epy_vak_11_z',
                                                'epy_vak_12','epy_vak_13_z', 'epy_vak_14', 
                                                'epy_vak_15_z', 'epy_vak_16','epy_vak_17_z', 'epy_vak_18'
                                                , 'epy_vak_19_z', 'epy_vak_20', 'epy_vak_21', 'epy_vak_22'
                                                , 'epy_vak_23_z', 'epy_vak_24', 'epy_vak_25_z', 'revac_20_03_s')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('epy_vak_01'  epy_vak_01,'epy_vak_02'  epy_vak_02,
        'epy_vak_04'  epy_vak_04,'epy_vak_05_z'  epy_vak_05_z,'epy_vak_06'  epy_vak_06,
        'epy_vak_07_z'  epy_vak_07_z,'epy_vak_08'  epy_vak_08,'epy_vak_09_z'  epy_vak_09_z,
        'epy_vak_10'  epy_vak_10,'epy_vak_11_z'  epy_vak_11_z,'epy_vak_12'  epy_vak_12,
        'epy_vak_13_z'  epy_vak_13_z,'epy_vak_14'  epy_vak_14,'epy_vak_15_z'  epy_vak_15_z,
        'epy_vak_16'  epy_vak_16,'epy_vak_17_z'  epy_vak_17_z,'epy_vak_18'  epy_vak_18,
        'epy_vak_19_z'  epy_vak_19_z,'epy_vak_20'  epy_vak_20,'epy_vak_21'  epy_vak_21,
        'epy_vak_22'  epy_vak_22,'epy_vak_23_z'  epy_vak_23_z,'epy_vak_24'  epy_vak_24,
        'epy_vak_25_z'  epy_vak_25,'revac_20_03_s'  revac_20_03
        )
                )
        ORDER BY ORGANIZATION,TYPE
