SELECT a.AGNNAME "Наименование МО",a.AGNABBR "Мнемокод" 
            ,CASE 
              WHEN r.SENT =  1
                THEN 'Да'
                ELSE 'Нет'
             END "Отправлен на проверку"
          FROM PARUS.BLINDEXVALUES  d
            INNER JOIN PARUS.BLSUBREPORTS s
              ON (d.PRN = s.RN)
            INNER JOIN PARUS.BLREPORTS r
              ON (s.PRN = r.RN)
            INNER JOIN PARUS.AGNLIST a 
              ON (r.AGENT = a.rn)
            INNER JOIN PARUS.BLREPFORMED pf 
              ON (r.BLREPFORMED = pf.RN)
            INNER JOIN PARUS.BLREPFORM rf 
              ON (pf.PRN = rf.RN)
            INNER JOIN PARUS.BALANCEINDEXES bi 
              ON (d.BALANCEINDEX = bi.RN)
          WHERE rf.CODE = '43 COVID 19' -- наименование отчета
              AND r.BDATE = trunc(SYSDATE) -- отчет за сегодняшнюю дату
              AND bi.CODE = '43_covid_03' -- тип показателя, в котором указано наименование МО
              AND s.SAVE_RESULT = 0    -- кто не сохранил свой отчет    
          ORDER BY a.AGNABBR
