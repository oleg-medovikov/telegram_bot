select [Медицинская организация],[УНРЗ], [ФИО], [Дата рождения], [Дата создания РЗ]
    from cv_fedreg where [Степень тяжести] = ''
    and( [Диагноз] in ('U07.1','U07.2') or  [Диагноз]  like 'J1[2-8]%')
