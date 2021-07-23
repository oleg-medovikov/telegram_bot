select new.[Type_Therapy], new.MO_Name_Parus,
  new.[Count_70_COVID] - old.[Count_70_COVID] as 'Count_70_COVID_week', new.[Count_70_COVID],
  new.[Count_70_Pnev] - old.[Count_70_Pnev] as 'Count_70_Pnev_week', new.[Count_70_Pnev],
  new.[Count_Diary] - old.[Count_Diary] as 'Count_Diary_week', new.[Count_Diary],
  new.[Count_SNILS] - old.[Count_SNILS] as 'Count_SNILS_week', new.[Count_SNILS],
  new.[Count_50_days] - old.[Count_50_days] as 'Count_50_days_week', new.[Count_50_days]
from (
  SELECT [Type_Therapy]
      ,[MO_Name_Parus]
      ,MIN([Count_70_COVID]) AS 'Count_70_COVID'
      ,MIN([Count_70_Pnev]) AS 'Count_70_Pnev'
      ,MIN([Count_Diary]) AS 'Count_Diary'
      ,MIN([Count_SNILS]) AS 'Count_SNILS'
      ,MIN([Count_50_days]) AS 'Count_50_days'
  FROM [COVID].[mz].[Report_50]
  where [Date_Report] = (select max([Date_Report]) from [COVID].[mz].[Report_50] where Name_Day='Thursday')
  GROUP BY [Type_Therapy], [MO_Name_Parus]) new
inner join (
  SELECT [Type_Therapy]
      ,[MO_Name_Parus]
      ,MIN([Count_70_COVID]) AS 'Count_70_COVID'
      ,MIN([Count_70_Pnev]) AS 'Count_70_Pnev'
      ,MIN([Count_Diary]) AS 'Count_Diary'
      ,MIN([Count_SNILS]) AS 'Count_SNILS'
      ,MIN([Count_50_days]) AS 'Count_50_days'
  FROM [COVID].[mz].[Report_50]
  where [Date_Report] =  ( select max([Date_Report]) from [COVID].[mz].[Report_50] where Name_Day='Thursday' 
  and [Date_Report] != (select max([Date_Report]) from [COVID].[mz].[Report_50] where Name_Day='Thursday'))
  GROUP BY [Type_Therapy], [MO_Name_Parus]) old
  on (new.[MO_Name_Parus] = old.[MO_Name_Parus] and new.Type_Therapy = old.Type_Therapy )
  ORDER BY Count_70_COVID_week
