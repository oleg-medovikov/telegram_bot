select f.*,isnull(c.[Примечания],'') as [Примечания]  
from (
select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
from robo.v_FedReg
where [Исход заболевания] in ('', 'Перевод пациента в другую МО',
                                'Перевод пациента на амбулаторное лечение',
                                'Перевод пациента на стационарное лечение')
    and [МО прикрепления] = '' ) as f
    left join robo.snils_comment c
     on (f.УНРЗ = c.УНРЗ)
