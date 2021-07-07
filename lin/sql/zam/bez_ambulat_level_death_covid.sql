select fr.* from 
(select dbo.get_Gid(idPatient) as 'Gid',[УНРЗ],[ФИО],[Дата рождения],[СНИЛС],[Вид лечения],
[МО прикрепления] as 'Медицинская организация', [Исход заболевания],[Дата исхода заболевания],[Диагноз],[Диагноз установлен]
    from dbo.cv_fedreg
        where [Медицинская организация] in (
'СПб ГБУЗ "Городская больница 40"','СПб ГБУЗ "Городская Покровская больница"','СПб ГБУЗ "ГМПБ 2"',
'СПб ГБУЗ "Городская Мариинская больница"','СПб ГБУЗ "Госпиталь для ветеранов войн"','СПб ГБУЗ "ДГКБ 5 им. Н.Ф.Филатова"',
'СПб ГБУЗ "Городская больница 38 им. Н.А.Семашко"','СПб ГБУЗ "Городская больница Святого Великомученика Георгия"',
'СПб ГКУЗ "ГПБ 3 им. И.И.Скворцова-Степанова"','СПб ГБУЗ "Александровская больница"','СПб ГБУЗ "Клиническая инфекционная больница им. С.П. Боткина"',
'ФГБУ НМИЦ им. В.А. Алмазова Минздрава России','ФГБОУ ВО ПСПбГМУ им. И.П.Павлова Минздрава России','СПб ГБУЗ "Городская больница 20"',
'СПб ГБУЗ "Николаевская больница"','СПб ГБУЗ "Елизаветинская больница"','ФГБУ "СЗОНКЦ им.Л.Г.Соколова ФМБА России"',
'СПб ГБУЗ "Городская больница Святого Праведного Иоанна Кронштадтского"','СПб ГБУЗ "Городская больница 15"','СПб ГБУЗ "ДГБ Св. Ольги"',
'ФГБОУ ВО СЗГМУ им. И.И. Мечникова Минздрава России','ФГБУ НМИЦ ТО им. Р.Р. Вредена Минздрава России',
'СПб ГБУЗ "Городская клиническая больница 31"','СПб ГБУЗ "Городская больница 26"','СПб ГБУЗ "Родильный дом 16"',
'СПб ГБУЗ "Санкт-Петербургская психиатрическая больница 1 им.П.П.Кащенко"','СПб ГБУЗ "Городская больница 33"',
'ФКУ "Санкт-Петербургская ПБСТИН" Минздрава России','СПб ГБУЗ Клиническая больница Святителя Луки','СПб ГБУЗ "Родильный дом 13"',
'ГБУ СПб НИИ СП им. И.И. Джанелидзе','ГБУЗ СПб КНпЦСВМП(о)','СПб ГБУЗ "Городская больница 14"',
'СПб ГБУЗ "Городской гериатрический медико-социальный центр"','СПб ГБУЗ "ДИБ 3"','СПб ГБУЗ "Детская городская больница 22"',
'СПБ ГБУЗ "Введенская больница"','СПБ ГБУЗ "ГНБ"','ФГБУ "СПб НИИФ" Минздрава России','ЧУЗ КБ РЖД-МЕДИЦИНА Г. С-ПЕТЕРБУРГ"',
'СПб ГБУЗ "Городская больница 28 "Максимилиановская"','СПб ГБУЗ "Родильный дом 18"','ФГБОУ ВО СПбГПМУ Минздрава России',
'СПб ГБУЗ "Родильный дом 10"','СПб ГБУЗ "Городская больница 9"','ФГБУ "НМИЦ онкологии им. Н.Н.Петрова" Минздрава России',
'СПБ ГБУЗ "Детский городской многопрофильный клинический специализированный центр высоких медицинских технологий"','СПб ГБУЗ "Родильный дом 17"',
'СПб ГБУЗ "Городская туберкулезная больница 2"','СПб ГБУЗ "Родильный дом 9"','ФГБУ ВЦЭРМ ИМ. А.М. Никифорова МЧС России',
'СПб ГБУЗ Клиническая инфекционная больница им.С.П.Боткина','СПб ГБУЗ "ППТД"','СПб ГБУЗ "Клиническая ревматологическая больница 25"',
'СПб ГБУЗ "Центр СПИД и инфекционных заболеваний"','СПб ГБУЗ "ГКОД"' )
        and [Исход заболевания] in ('Перевод пациента в другую МО','Перевод пациента на амбулаторное лечение','Перевод пациента на стационарное лечение')
        and [МО прикрепления] != ''
        and  DATEDIFF (day,[Диагноз установлен],[Дата исхода заболевания])  > 7  ) as fr
    left join ( select dbo.get_Gid(idPatient) as 'Gid',*  from cv_umsrs )as um 
        on (fr.Gid = um.Gid)
            where um.Gid is not null
and (um.Kod_MKB_10_a = 'U07.1' or um.Kod_MKB_10_a = 'U07.1' or um.Kod_MKB_10_b = 'U07.1' or um.Kod_MKB_10_v  = 'U07.1' or um.Kod_MKB_10_g = 'U07.1' or Kod_II = 'U07.1')
order by fr.[Медицинская организация] desc,fr.[Дата исхода заболевания]