select [УНРЗ],[Медицинская организация],[Диагноз установлен],[Дата исхода заболевания]
from dbo.cv_fedreg
where [Исход заболевания] = ''
    and DATEDIFF (day,[Диагноз установлен],getdate()) > 30
