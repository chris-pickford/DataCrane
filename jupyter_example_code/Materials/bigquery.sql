SELECT 
  TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL h.time MILLISECOND) AS event_time
  ,fullVisitorId 
  ,visitNumber
  ,h.page.pagePath
  ,h.page.searchKeyword
  ,h.page.pageTitle
  ,totals.transactions


FROM `project_ID.Dataset.table_*`,UNNEST(hits) as h
WHERE
 _TABLE_SUFFIX BETWEEN '20180901' AND '20181016'
