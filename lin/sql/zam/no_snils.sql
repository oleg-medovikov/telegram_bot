select f.УНРЗ,f.ФИО,f.[Дата рождения],f.[Медицинская организация]
,isnull(c.[Примечания],'') as [Примечания]
  from(
   select dbo.get_Gid(idPatient) as gid,[УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
      from robo.v_FedReg
      where [СНИЛС] = 'Не идентифицирован' ) f
  left join robo.snils_comment c
     on (f.УНРЗ = c.УНРЗ)
  left join (select dbo.get_Gid(idPatient) as gid  from  cv_umsrs) as um  on (f.gid = um.gid )
	where um.gid is null
