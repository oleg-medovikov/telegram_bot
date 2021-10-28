select f.*,isnull(c.[Примечания],'') as [Примечания]  
  from(
   select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
      from robo.v_FedReg
      where [СНИЛС] = 'Не идентифицирован' ) f
  left join robo.snils_comment c
     on (f.УНРЗ = c.УНРЗ)
