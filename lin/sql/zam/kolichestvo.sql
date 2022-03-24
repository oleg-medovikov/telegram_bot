select kol.*, dubli.[Количество дублей] from (
    SELECT [Медицинская организация],[Принадлежность],[Тип организации]
        , count(*) as 'Всего записей'
        , count (distinct dbo.get_Gid(idPatient)) as 'Уникальных пациентов'
        from robo.v_FedReg
            where [Тип организации] is not null
            group by [Медицинская организация],[Принадлежность],[Тип организации] ) as kol
    left join  (select itog.[Первая организация], count(*) as 'Количество дублей' from (
					select distinct
					fr1.УНРЗ as 'Первое УНРЗ', fr1.[Медицинская организация] as 'Первая организация'
						from (select distinct dbo.get_Gid(IdPatient) as 'Gid',[idFedreg],[УНРЗ],[ФИО],[Диагноз установлен],[Медицинская организация],[СНИЛС],[Исход заболевания]
									from  robo.v_FedReg) as fr1

						inner join (select  distinct dbo.get_Gid(IdPatient) as 'Gid',[idFedreg],[УНРЗ],[ФИО],[Диагноз установлен],[Медицинская организация],[СНИЛС],[Исход заболевания]
											from robo.v_FedReg ) as fr2
							on(
							fr1.Gid = fr2.Gid and fr1.УНРЗ <> fr2.УНРЗ 
							and fr1.[idFedreg] - fr2.[idFedreg] < 0
							and fr1.[Медицинская организация] = fr2.[Медицинская организация]
							and(fr1.[СНИЛС] = 'Не идентифицирован' or fr2.[СНИЛС] = 'Не идентифицирован') 
							and  abs(DATEDIFF (day,fr1.[Диагноз установлен],fr2.[Диагноз установлен])) < 60
							) ) as  itog 
        group by itog.[Первая организация] ) as dubli
    on (kol.[Медицинская организация] = dubli.[Первая организация])
