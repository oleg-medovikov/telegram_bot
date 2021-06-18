SELECT
         [Вид лечения]
         ,count(distinct [УНРЗ]) As 'Всего'
         ,count(distinct case when  (age >=60 ) then [УНРЗ] end) As 'Заболевшие в возрасте 60+'
         ,count(distinct case when  (age >=70 ) then [УНРЗ] end) As 'Заболевшие в возрасте 70+'
  FROM (SELECT [Диагноз]
                           ,[Вид лечения]
                           ,[УНРЗ]
                           , CASE 
                                        when [Диагноз] = 'U07.1'
                                               then 'U1'
                                        when [Диагноз] = 'U07.2'
                                               then 'U2'
                                        when [Диагноз] like '%J1[2-8]%'
                                               then 'J'
                             end 'Diagn'
                           ,dbo.f_calculation_age([Дата рождения],[Диагноз установлен]) As 'age'
                    FROM [COVID].[dbo].[cv_fedreg]
                    where ([Диагноз] like '%U07%'
                                  OR [Диагноз] like '%J1[2-8]%')
                                  and Isnull([Дата исхода заболевания],'') ='') As dan
             GROUP BY [Вид лечения]
