SELECT STRVAL ORGANIZATION
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
WHERE rf.CODE = '49 COVID 19'
and r.BDATE in (SELECT * FROM (
				SELECT  DISTINCT r.BDATE FROM PARUS.BLTBLVALUES v
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
                WHERE rf.code = '49 COVID 19' AND r.BDATE < trunc(SYSDATE) + 5 
                order BY r.BDATE DESC )
                WHERE  ROWNUM <= 2 )
AND bi.CODE = '49_covid_01_4'
GROUP BY STRVAL
HAVING count(r.BDATE) < 2 

