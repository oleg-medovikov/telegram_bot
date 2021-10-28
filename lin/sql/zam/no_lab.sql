select fr.[Медицинская организация], fr.[УНРЗ],fr.[ФИО],fr.[Дата рождения],fr.Диагноз
  from ( select * from robo.v_FedReg where [Диагноз] ='U07.1') as fr
    left join (select distinct УНРЗ from [dbo].[cv_fedreg_lab] where [Результат теста (положительный/ отрицательный)] = 1 ) as lab
        on (fr.УНРЗ = lab.УНРЗ)
            where lab.УНРЗ is null



