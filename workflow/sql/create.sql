CREATE TABLE Analytics.WorkflowProcessAvg (
    date DATE,
	analyticskey VARCHAR, 
	processid BIGINT,
    processversionid BIGINT,
    total BIGINT,
    totalcompleted BIGINT,
    totalremoved BIGINT,
    totalduration BIGINT,
    PRIMARY KEY(date, analyticskey, processid, processversionid)
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
    processid bigint,    
    totalduration bigint,
    total bigint,
    PRIMARY KEY(date, analyticskey, taskid, processid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.Workflows (
    processid bigint,
 	analyticskey VARCHAR, 
    deleted boolean,
    active boolean,
    title TEXT,
    PRIMARY KEY(processid, analyticskey)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;