select kol.*, dubli.[Количество дублей] from (
    SELECT [Тип организации],[Медицинская организация]
        , count(*) as 'Всего записей'
        , count (distinct dbo.get_Gid(idPatient)) as 'Уникальных пациентов'
        from robo.v_FedReg
            where [Тип организации] is not null
            group by [Тип организации],[Медицинская организация] ) as kol

    full join  (select itog.[Первая организация], count(*) as 'Количество дублей' from (
                    select row_number() over(order by cast(fr1.УНРЗ as bigint)  + cast(fr2.УНРЗ as bigint) ) as 'номер' 
		                    ,fr1.УНРЗ as 'Первое УНРЗ', fr1.ФИО, fr1.[Медицинская организация] as 'Первая организация'
		                    , fr2.УНРЗ as 'Второе УНРЗ',fr2.[Медицинская организация]  as 'Вторая организация'
	                    from (select distinct dbo.get_Gid(IdPatient) as 'Gid',[УНРЗ],[ФИО],[Диагноз установлен],[Медицинская организация],[СНИЛС],[Исход заболевания]
                                    from  robo.v_FedReg) as fr1
                            inner join (select  distinct dbo.get_Gid(IdPatient) as 'Gid',[УНРЗ],[ФИО],[Диагноз установлен],[Медицинская организация],[СНИЛС],[Исход заболевания]
                                            from robo.v_FedReg ) as fr2
                            on(fr1.Gid = fr2.Gid and fr1.УНРЗ <> fr2.УНРЗ and fr1.[Медицинская организация] = fr2.[Медицинская организация]
                                    and(fr1.[СНИЛС] = 'Не идентифицирован' or fr2.[СНИЛС] = 'Не идентифицирован') 
                                    and abs(DATEDIFF (day,fr1.[Диагноз установлен],fr2.[Диагноз установлен])) < 60
                                    and ( (fr1.[Диагноз установлен] < fr2.[Диагноз установлен] and fr1.[Исход заболевания] != 'Выздоровление' )
	                                        or
	                                    (fr1.[Диагноз установлен] > fr2.[Диагноз установлен] and fr2.[Исход заболевания] != 'Выздоровление' )) 
                                )) as  itog 
        where itog.[номер] % 2 = 1
        group by itog.[Первая организация] ) as dubli

    on (kol.[Медицинская организация] = dubli.[Первая организация])
