CREATE TABLE Analytics.WorkflowProcessAvg(
	analyticskey VARCHAR,
	date DATE,
	processid BIGINT,
	processversionid BIGINT,
	totalcompleted BIGINT,
	totalduration BIGINT,
	totalremoved BIGINT,
	totalstarted BIGINT,
	PRIMARY KEY(analyticskey, date, processid, processversionid)
)
WITH compaction = { 'class': 'DateTieredCompactionStrategy' }
AND default_time_to_live = 7776000;

CREATE INDEX workflowprocessavg_processid ON Analytics.WorkflowProcessAvg(processid);

CREATE TABLE Analytics.WorkflowEntities(
	entity TEXT,
	id bigint,
	name TEXT,
	PRIMARY KEY(entity, id)
)
WITH compaction = { 'class': 'DateTieredCompactionStrategy' }
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.WorkflowTaskAvg(
	analyticskey VARCHAR,
	date DATE,
	taskid bigint,
	processversionid bigint,
	totalduration bigint,
	total bigint,
	PRIMARY KEY(analyticskey, date, taskid, processversionid)
)
WITH compaction = { 'class': 'DateTieredCompactionStrategy' }
AND default_time_to_live = 7776000;

CREATE INDEX workflowtaskavg_processversionid ON Analytics.WorkflowTaskAvg(processversionid);

CREATE TABLE Analytics.Workflows(
	analyticskey VARCHAR,
	processid bigint,
	deleted boolean,
	active boolean,
	title TEXT,
	PRIMARY KEY(analyticskey, processid)
)
WITH compaction = { 'class': 'DateTieredCompactionStrategy' }
AND default_time_to_live = 7776000;

CREATE INDEX workflows_deleted ON Analytics.Workflows(deleted);
CREATE INDEX workflows_processid ON Analytics.Workflows(processid);