select [УНРЗ],[ФИО],[Дата рождения],[Медицинская организация]
from cv_fedreg
where [Исход заболевания] in ('', 'Перевод пациента в другую МО',
                                'Перевод пациента на амбулаторное лечение',
                                'Перевод пациента на стационарное лечение')
    and [МО прикрепления] = ''