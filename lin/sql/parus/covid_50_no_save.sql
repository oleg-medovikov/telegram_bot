SELECT DISTINCT a.AGNNAME "Наименование МО",a.AGNABBR "Мнемокод" 
            ,CASE 
              WHEN r.SENT =  1
                THEN 'Да'
                ELSE 'Нет'
             END "Отправлен на проверку"
             ,to_char(r.BDATE, 'DD.MM.YYYY') AS "Дата отчета"
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
          WHERE rf.CODE = '50 COVID 19' -- наименование отчёта
              AND r.BDATE = (SELECT max(r.BDATE) FROM PARUS.BLTBLVALUES v
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
				WHERE rf.code = '50 COVID 19') 
              AND s.SAVE_RESULT = 0    -- кто не сохранил свой отчёт
          ORDER BY a.AGNABBR
