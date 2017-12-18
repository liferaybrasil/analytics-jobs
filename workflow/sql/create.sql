CREATE TABLE analytics.workflowprocess(
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

CREATE INDEX workflowprocess_processid ON analytics.workflowprocess(processid);

CREATE TABLE analytics.workflowtask(
	analyticskey VARCHAR,
	date DATE,
	processversionid BIGINT,
	taskid BIGINT,
	name TEXT,
	total BIGINT,
	totalduration BIGINT,
	PRIMARY KEY(analyticskey, date, processversionid, taskid)
)
WITH compaction = { 'class': 'DateTieredCompactionStrategy' }
AND default_time_to_live = 7776000;

CREATE INDEX workflowtasK_processversionid ON analytics.workflowtask(processversionid);

CREATE TABLE analytics.workflow(
	analyticskey VARCHAR,
	processid BIGINT,
	active BOOLEAN,
	deleted BOOLEAN,
	title TEXT,
	PRIMARY KEY(analyticskey, processid)
)
WITH compaction = { 'class': 'DateTieredCompactionStrategy' }
AND default_time_to_live = 7776000;

CREATE INDEX workflow_deleted ON analytics.workflow(deleted);
CREATE INDEX workflow_processid ON analytics.workflow(processid);