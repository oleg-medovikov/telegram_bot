SELECT day, substr(tvsp ,1,INSTR(tvsp , ' ')-1) dist,  tvsp, V_1, V_2  
	FROM (
		SELECT 
                r.BDATE day,
                a.AGNNAME organization,
            	ro.NUMB row_index ,
            	i.CODE pokazatel,
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
        and r.BDATE BETWEEN TO_DATE('30-01-2021','DD-MM-YYYY')  and trunc(sysdate)
        and i.CODE in ('Vaccin_TVSP','Vaccin_tvsp_18', 'Vaccin_tvsp_19_day' )
        )
        pivot
        (
        max(value)
        FOR POKAZATEL IN ('Vaccin_TVSP' tvsp,'Vaccin_tvsp_18' V_1, 'Vaccin_tvsp_19_day' V_2 )
        )
        ORDER BY day, ROW_INDEX
