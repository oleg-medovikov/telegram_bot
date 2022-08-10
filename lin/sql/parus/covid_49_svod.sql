SELECT  DAY,ORGANIZATION,pok1,
	    nvl(cast(pok2 as int),0) pok2,nvl(cast(pok3 as int),0) pok3,
	    nvl(cast(pok4 as int),0) pok4,nvl(cast(pok5 as int),0) pok5,
	    nvl(cast(pok6 as int),0) pok6,nvl(cast(pok7 as int),0) pok7
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
WHERE rf.CODE = '49 COVID 19'
and r.BDATE = ( SELECT max( r.BDATE) FROM PARUS.BLTBLVALUES v
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
                WHERE rf.code = '49 COVID 19' AND r.BDATE < trunc(SYSDATE) + 5 )
order by  d.BALANCEINDEX
)
pivot
(
MIN(value)
FOR POKAZATEL IN ('49_covid_01_4' pok1,'49_covid_02_4' pok2,'49_covid_03_4' pok3,'49_covid_04_4' pok4,'49_covid_05_4' pok5,'49_covid_06_4' pok6,'49_covid_07_4' pok7)
)
where pok1 IS NOT null
ORDER BY pok1
