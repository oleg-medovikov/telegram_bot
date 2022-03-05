SELECT DISTINCT a.AGNNAME ORGANIZATION
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
WHERE rf.code = 'ДистанцКонсультации'
AND r.BDATE  = trunc(SYSDATE) -1 
