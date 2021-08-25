SELECT   med.[Id]
    ,med.[OrganizationId]
    ,REPLACE(REPLACE(REPLACE(med.[FullName], char(10), ' '), char(9), ' '), char(13), ' ') AS 'FullName'
    ,REPLACE(REPLACE(REPLACE(med.[ShortName], char(10), ' '), char(9), ' '), char(13), ' ') AS 'ShortName'
    ,REPLACE(REPLACE(REPLACE(med.[Code], char(10), ' '), char(9), ' '), char(13), ' ') AS 'Code'
    ,cast(ISNULL(parus.[IdentifierId], '') as int) AS 'IdentifierIdParus'
    ,ISNULL(analitic.[IdentifierId], '') AS 'IdentifierIdAnalitic'
  FROM [NsiBase].[dbo].[MedObjects] AS med
    LEFT JOIN (SELECT [MedObjectId]
              ,[IdentifierId]
            FROM [NsiBase].[dbo].[OrgIdentifiers]
            WHERE [IdentitySystemId] = 24) AS parus
      ON (parus.[MedObjectId] = med.[OrganizationId])
    LEFT JOIN (SELECT [MedObjectId]
              ,[IdentifierId]
            FROM [NsiBase].[dbo].[OrgIdentifiers]
            WHERE [IdentitySystemId] = 11) AS analitic
      ON (analitic.[MedObjectId] = med.[OrganizationId])
  WHERE (parus.IdentifierId is not null
      OR analitic.IdentifierId is not null)
      AND [OrganizationId] = med.[Id]
  ORDER BY  med.[Id]
