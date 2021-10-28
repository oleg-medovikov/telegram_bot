SELECT ORGANIZATION, 'Пункт вакцинации' type,  substr(EVC_TVSP_2_02 ,1,INSTR(EVC_TVSP_2_02 , ' ')-1) dist, 
        REPLACE(substr(EVC_TVSP_2_02 ,INSTR(EVC_TVSP_2_02 , ' ')+1, length(EVC_TVSP_2_02)),'район ','') EVC_TVSP_2_02,
        nvl(cast(EVC_TVSP_2_04 as int),0)  EVC_TVSP_2_04,
        nvl(cast(EVC_TVSP_2_05_z as int),0)  EVC_TVSP_2_05_z,nvl(cast(EVC_TVSP_2_06 as int),0)  EVC_TVSP_2_06,
        nvl(cast(EVC_TVSP_2_07_z as int),0)  EVC_TVSP_2_07_z,nvl(cast(EVC_TVSP_2_08 as int),0)  EVC_TVSP_2_08,
        nvl(cast(EVC_TVSP_2_09_z as int),0)  EVC_TVSP_2_09_z,nvl(cast(EVC_TVSP_2_10 as int),0)  EVC_TVSP_2_10,
        nvl(cast(EVC_TVSP_2_11_z as int),0)  EVC_TVSP_2_11_z,nvl(cast(EVC_TVSP_2_12 as int),0)  EVC_TVSP_2_12,
        nvl(cast(EVC_TVSP_2_13_z as int),0)  EVC_TVSP_2_13_z,nvl(cast(EVC_TVSP_2_14 as int),0)  EVC_TVSP_2_14,
        nvl(cast(EVC_TVSP_2_15_z as int),0)  EVC_TVSP_2_15_z,nvl(cast(EVC_TVSP_2_16 as int),0)  EVC_TVSP_2_16,
        nvl(cast(EVC_TVSP_2_17_z as int),0)  EVC_TVSP_2_17_z,nvl(cast(EVC_TVSP_2_18 as int),0)  EVC_TVSP_2_18,
        nvl(cast(EVC_TVSP_2_19_z as int),0)  EVC_TVSP_2_19_z,
        nvl(cast(EVC_TVSP_2_20 as int),0)  EVC_TVSP_2_20,nvl(cast(EVC_TVSP_2_21 as int),0)  EVC_TVSP_2_21,
        nvl(cast(EVC_TVSP_2_22 as int),0)  EVC_TVSP_2_22,nvl(cast(EVC_TVSP_2_23_z as int),0)  EVC_TVSP_2_23_z, 
        nvl(cast(EVC_TVSP_2_24 as int),0)  EVC_TVSP_2_24,nvl(cast(EVC_TVSP_2_25 as int),0)  EVC_TVSP_2_25,
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
					                and i.CODE in ('EVC_TVSP_2_05_z') 
										) WHERE num = 1)
                 and i.CODE in ('EVC_TVSP_2_02','EVC_TVSP_2_04','EVC_TVSP_2_05_z',
                                                'EVC_TVSP_2_06','EVC_TVSP_2_07_z', 'EVC_TVSP_2_08', 
                                                'EVC_TVSP_2_09_z', 'EVC_TVSP_2_10', 'EVC_TVSP_2_11_z',
                                                'EVC_TVSP_2_12','EVC_TVSP_2_13_z', 'EVC_TVSP_2_14', 
                                                'EVC_TVSP_2_15_z', 'EVC_TVSP_2_16','EVC_TVSP_2_17_z', 'EVC_TVSP_2_18'
                                                , 'EVC_TVSP_2_19_z', 'EVC_TVSP_2_20', 'EVC_TVSP_2_21', 'EVC_TVSP_2_22'
                                                , 'EVC_TVSP_2_23_z', 'EVC_TVSP_2_24', 'EVC_TVSP_2_25_z', 'revac_20_01')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('EVC_TVSP_2_02'  EVC_TVSP_2_02,
        'EVC_TVSP_2_04'  EVC_TVSP_2_04,
        'EVC_TVSP_2_05_z'  EVC_TVSP_2_05_z,'EVC_TVSP_2_06'  EVC_TVSP_2_06,
        'EVC_TVSP_2_07_z'  EVC_TVSP_2_07_z,
        'EVC_TVSP_2_08'  EVC_TVSP_2_08,
        'EVC_TVSP_2_09_z'  EVC_TVSP_2_09_z,'EVC_TVSP_2_10'  EVC_TVSP_2_10,
        'EVC_TVSP_2_11_z'  EVC_TVSP_2_11_z,
        'EVC_TVSP_2_12'  EVC_TVSP_2_12,
        'EVC_TVSP_2_13_z'  EVC_TVSP_2_13_z,'EVC_TVSP_2_14'  EVC_TVSP_2_14,
        'EVC_TVSP_2_15_z'  EVC_TVSP_2_15_z,
        'EVC_TVSP_2_16'  EVC_TVSP_2_16,
        'EVC_TVSP_2_17_z'  EVC_TVSP_2_17_z,'EVC_TVSP_2_18'  EVC_TVSP_2_18,
        'EVC_TVSP_2_19_z'  EVC_TVSP_2_19_z,
        'EVC_TVSP_2_20'  EVC_TVSP_2_20,'EVC_TVSP_2_21'  EVC_TVSP_2_21,
        'EVC_TVSP_2_22'  EVC_TVSP_2_22, 'EVC_TVSP_2_23_z'  EVC_TVSP_2_23_z,
        'EVC_TVSP_2_24'  EVC_TVSP_2_24,'EVC_TVSP_2_25_z'  EVC_TVSP_2_25,'revac_20_01'  revac_20_01
        )
                )
        UNION
        SELECT ORGANIZATION, 'Медицинская организация' TYPE, REPLACE (EVC_TVSP_01,' район ','') dist, EVC_TVSP_02,
        nvl(cast(EVC_TVSP_04 as int),0)  EVC_TVSP_04,nvl(cast(EVC_TVSP_05_z as int),0)  EVC_TVSP_05_z,
        nvl(cast(EVC_TVSP_06 as int),0)  EVC_TVSP_06,nvl(cast(EVC_TVSP_07_z as int),0)  EVC_TVSP_07_z,
        nvl(cast(EVC_TVSP_08 as int),0)  EVC_TVSP_08,nvl(cast(EVC_TVSP_09_z as int),0)  EVC_TVSP_09_z,
        nvl(cast(EVC_TVSP_10 as int),0)  EVC_TVSP_10,nvl(cast(EVC_TVSP_11_z as int),0)  EVC_TVSP_11_z,
        nvl(cast(EVC_TVSP_12 as int),0)  EVC_TVSP_12,nvl(cast(EVC_TVSP_13_z as int),0)  EVC_TVSP_13_z,
        nvl(cast(EVC_TVSP_14 as int),0)  EVC_TVSP_14,nvl(cast(EVC_TVSP_15_z as int),0)  EVC_TVSP_15_z,
        nvl(cast(EVC_TVSP_16 as int),0)  EVC_TVSP_16,nvl(cast(EVC_TVSP_17_z as int),0)  EVC_TVSP_17_z,
        nvl(cast(EVC_TVSP_18 as int),0)  EVC_TVSP_18,nvl(cast(EVC_TVSP_19_z as int),0)  EVC_TVSP_19_z,
        nvl(cast(EVC_TVSP_20 as int),0)  EVC_TVSP_20,nvl(cast(EVC_TVSP_21   as int),0)  EVC_TVSP_21,
        nvl(cast(EVC_TVSP_22 as int),0)  EVC_TVSP_22,nvl(cast(EVC_TVSP_23_z as int),0)  EVC_TVSP_23_z,
        nvl(cast(EVC_TVSP_24 as int),0)  EVC_TVSP_24,nvl(cast(EVC_TVSP_25 as int),0)  EVC_TVSP_25,
        nvl(cast(revac_20_04 as int),0)  revac_20_04
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
                 and bi.CODE in ('EVC_TVSP_01','EVC_TVSP_02','EVC_TVSP_04','EVC_TVSP_05_z',
                                                'EVC_TVSP_06','EVC_TVSP_07_z', 'EVC_TVSP_08', 
                                                'EVC_TVSP_09_z', 'EVC_TVSP_10', 'EVC_TVSP_11_z',
                                                'EVC_TVSP_12','EVC_TVSP_13_z', 'EVC_TVSP_14', 
                                                'EVC_TVSP_15_z', 'EVC_TVSP_16','EVC_TVSP_17_z', 'EVC_TVSP_18'
                                                , 'EVC_TVSP_19_z', 'EVC_TVSP_20', 'EVC_TVSP_21', 'EVC_TVSP_22'
                                                , 'EVC_TVSP_23_z', 'EVC_TVSP_24', 'EVC_TVSP_25_z', 'revac_20_04_s')
                )
                pivot
                (
                max(value)
                FOR POKAZATEL IN ('EVC_TVSP_01'  EVC_TVSP_01,'EVC_TVSP_02'  EVC_TVSP_02,
        'EVC_TVSP_04'  EVC_TVSP_04,'EVC_TVSP_05_z'  EVC_TVSP_05_z,'EVC_TVSP_06'  EVC_TVSP_06,
        'EVC_TVSP_07_z'  EVC_TVSP_07_z,'EVC_TVSP_08'  EVC_TVSP_08,'EVC_TVSP_09_z'  EVC_TVSP_09_z,
        'EVC_TVSP_10'  EVC_TVSP_10,'EVC_TVSP_11_z'  EVC_TVSP_11_z,'EVC_TVSP_12'  EVC_TVSP_12,
        'EVC_TVSP_13_z'  EVC_TVSP_13_z,'EVC_TVSP_14'  EVC_TVSP_14,'EVC_TVSP_15_z'  EVC_TVSP_15_z,
        'EVC_TVSP_16'  EVC_TVSP_16,'EVC_TVSP_17_z'  EVC_TVSP_17_z,'EVC_TVSP_18'  EVC_TVSP_18,
        'EVC_TVSP_19_z'  EVC_TVSP_19_z,'EVC_TVSP_20'  EVC_TVSP_20,'EVC_TVSP_21'  EVC_TVSP_21,
        'EVC_TVSP_22'  EVC_TVSP_22,'EVC_TVSP_23_z'  EVC_TVSP_23_z,'EVC_TVSP_24'  EVC_TVSP_24,
        'EVC_TVSP_25_z'  EVC_TVSP_25,'revac_20_04_s'  revac_20_04
        )
                )
        ORDER BY ORGANIZATION,TYPE
