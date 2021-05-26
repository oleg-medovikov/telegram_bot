SELECT CAST(r.BDATE AS varchar(30))  day,
                a.AGNABBR code,
                a.AGNNAME  organization,
                sum(CASE WHEN NUMVAL = 0 THEN 1 ELSE 0 END ) nulls_in_itog
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
        WHERE  rf.CODE = '43 COVID 19'
        and bi.CODE in ('43_covid_05','43_covid_07','43_covid_09_2')
        AND r.BDATE IN ( trunc(SYSDATE),  trunc(SYSDATE-1),  trunc(SYSDATE-2))
        GROUP BY r.BDATE,a.AGNABBR,a.AGNNAME
        HAVING sum(CASE WHEN NUMVAL = 0 THEN 1 ELSE 0 END ) > 1
