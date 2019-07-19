SELECT [ID]
      ,[TrafficDate]
      ,[KeywordID]
      ,[Keyword]
      ,[AdGroupID]
      ,[AdGroup]
  FROM [database].[table]
  WHERE TrafficDate BETWEEN '@StartDate' AND '@EndDate'
  