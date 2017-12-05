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

CREATE TABLE Analytics.WorkflowEntities (
    entity TEXT,
    id bigint,
    name TEXT,
    PRIMARY KEY(entity, id)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.WorkflowTaskAvg (
 	date DATE,
 	analyticskey VARCHAR, 
    taskid bigint,
    processversionid bigint,    
    totalduration bigint,
    total bigint,
    PRIMARY KEY(date, analyticskey, taskid, processversionid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;