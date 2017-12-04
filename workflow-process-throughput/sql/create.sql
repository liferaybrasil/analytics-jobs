CREATE TABLE Analytics.WorkflowProcessAvg (
    date DATE,
	analyticskey VARCHAR, 
    processversionid BIGINT,
    total BIGINT,
    totalcompleted BIGINT,
    totalremoved BIGINT,
    totalduration BIGINT,
    PRIMARY KEY(date, analyticskey, processversionid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;