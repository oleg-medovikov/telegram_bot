SELECT  DAY, pok_01, nvl(cast(pok_02 as int),0) pok_02,
	nvl(cast(pok_12 as int),0) pok_12,nvl(cast(pok_13 as int),0) pok_13,nvl(cast(pok_14 as int),0) pok_14,
    nvl(cast(pok_15 as int),0) pok_15,nvl(cast(pok_16 as int),0) pok_16,nvl(cast(pok_17 as int),0) pok_17,nvl(cast(pok_18 as int),0) pok_18,
    nvl(cast(pok_19 as int),0) pok_19,nvl(cast(pok_20 as int),0) pok_20,nvl(cast(pok_21 as int),0) pok_21,nvl(cast(pok_22 as int),0) pok_22,
    nvl(cast(pok_23 as int),0) pok_23,nvl(cast(pok_24 as int),0) pok_24,nvl(cast(pok_25 as int),0) pok_25,nvl(cast(pok_26 as int),0) pok_26,
    nvl(cast(pok_27 as int),0) pok_27,nvl(cast(pok_28 as int),0) pok_28,nvl(cast(pok_29 as int),0) pok_29,nvl(cast(pok_30 as int),0) pok_30,
    nvl(cast(pok_31 as int),0) pok_31,nvl(cast(pok_32 as int),0) pok_32,nvl(cast(pok_33 as int),0) pok_33,nvl(cast(pok_34 as int),0) pok_34,
    nvl(cast(pok_35 as int),0) pok_35,nvl(cast(pok_36 as int),0) pok_36,nvl(cast(pok_37 as int),0) pok_37,
    nvl(cast(pok_39 as int),0) pok_39,nvl(cast(pok_40 as int),0) pok_40,nvl(cast(pok_41 as int),0) pok_41,nvl(cast(pok_42 as int),0) pok_42,
    nvl(cast(pok_43 as int),0) pok_43,nvl(cast(pok_44 as float),0) pok_44,nvl(cast(pok_45 as float),0) pok_45,nvl(cast(pok_46 as float),0) pok_46,
    nvl(cast(pok_47 as float),0) pok_47,nvl(cast(pok_48 as float),0) pok_48,nvl(cast(pok_49 as float),0) pok_49,
    nvl(cast(pok_50 as float),0) pok_50,nvl(cast(pok_51 as float),0) pok_51,nvl(cast(pok_52 as float),0) pok_52,
    nvl(cast(pok_53 as float),0) pok_53,nvl(cast(pok_54 as float),0) pok_54,nvl(cast(pok_55 as float),0) pok_55,
    nvl(cast(pok_62 as float),0) pok_62,nvl(cast(pok_63 as float),0) pok_63,nvl(cast(pok_64 as float),0) pok_64,nvl(cast(pok_65 as float),0) pok_65,
    nvl(cast(pok_66 as float),0) pok_66,nvl(cast(pok_67 as float),0) pok_67
    FROM (
    SELECT 
            to_char(r.BDATE, 'DD.MM.YYYY')  day,
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
    WHERE rf.CODE = '29 COVID 19'
    and r.BDATE =  trunc(SYSDATE)
    and bi.CODE LIKE '29_covid_0%'
    order by  d.BALANCEINDEX 
    )
    pivot
    (
    MIN(value)
    FOR POKAZATEL IN ('29_covid_001' pok_01,'29_covid_002' pok_02, '29_covid_012' pok_12,
    '29_covid_013' pok_13,'29_covid_014' pok_14,'29_covid_015' pok_15,'29_covid_016' pok_16,
    '29_covid_017' pok_17,'29_covid_018' pok_18,'29_covid_019' pok_19,'29_covid_020' pok_20,
    '29_covid_021' pok_21,'29_covid_022' pok_22,'29_covid_023' pok_23,'29_covid_024' pok_24,
    '29_covid_025' pok_25,'29_covid_026' pok_26,'29_covid_027' pok_27,'29_covid_028' pok_28,
    '29_covid_029' pok_29,'29_covid_030' pok_30,'29_covid_031' pok_31,'29_covid_032' pok_32,
    '29_covid_033' pok_33,'29_covid_034' pok_34,'29_covid_035' pok_35,'29_covid_036' pok_36,
    '29_covid_037' pok_37,'29_covid_039' pok_39,'29_covid_040' pok_40,
    '29_covid_041' pok_41,'29_covid_042' pok_42,'29_covid_043' pok_43,'29_covid_044' pok_44,
    '29_covid_045' pok_45,'29_covid_046' pok_46,'29_covid_047' pok_47,'29_covid_048' pok_48,
    '29_covid_049' pok_49,'29_covid_050' pok_50,'29_covid_051' pok_51,'29_covid_052' pok_52,
    '29_covid_053' pok_53,'29_covid_054' pok_54,'29_covid_055' pok_55,
    '29_covid_062' pok_62,'29_covid_063' pok_63,'29_covid_064' pok_64,'29_covid_065' pok_65
    ,'29_covid_066' pok_66,'29_covid_067' pok_67)
    )
    WHERE POK_01 IS NOT NULL 
    ORDER BY POK_01