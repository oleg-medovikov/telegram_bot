select f.*,isnull(c.[Примечания],'') as [Примечания]  
  from(
   select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
      from cv_fedreg
      where [СНИЛС] = 'Не идентифицирован' ) f
  left join robo.snils_comment c
     on (f.УНРЗ = c.УНРЗ)
