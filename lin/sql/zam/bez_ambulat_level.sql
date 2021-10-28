select s.Gid,s.[УНРЗ],s.[ФИО],s.[Дата рождения],s.[СНИЛС],s.[Вид лечения],
	s.[МО прикрепления] as 'Медицинская организация',s.[Исход заболевания],s.[Дата исхода заболевания],
	s.[Диагноз],s.[Диагноз установлен],DATEDIFF(day,s.[Дата исхода заболевания],getdate()) as [Дней в статусе перевода]
	from
(select dbo.get_Gid(idPatient) as 'Gid',* 
	from robo.v_FedReg
	where [Исход заболевания] in ('Перевод пациента в другую МО',
								'Перевод пациента на амбулаторное лечение',
								'Перевод пациента на стационарное лечение')
			and [МО прикрепления] != ''
			and  [МО прикрепления]  in (select distinct [Медицинская организация] from robo.v_FedReg where [Субъект РФ] = 'г. Санкт-Петербург') 
	        and [Вид лечения] = 'Стационарное лечение'
            and DATEDIFF(day,[Дата исхода заболевания],getdate())  > 7
	) as s
 left join ( select dbo.get_Gid(idPatient) as 'Gid'  from cv_umsrs )as um
        on (s.Gid = um.Gid)
        where um.Gid is null
order by s.[МО прикрепления] desc,s.[Дата исхода заболевания]
