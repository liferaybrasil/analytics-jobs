CREATE TABLE Analytics.FormUserView (
	formid bigint,
	userid bigint,
	createdate timestamp,
	PRIMARY KEY(formid, userid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.FormUserStartedFilling (
	formid bigint,
	userid bigint,
	createdate timestamp,
	PRIMARY KEY(formid, userid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.FormUserSubmitted (
	formid bigint,
	userid bigint,
	createdate timestamp,
	PRIMARY KEY(formid, userid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.FormsViewsStarted (
	formid bigint,
	createdate date,
	views bigint,
	started bigint,
	PRIMARY KEY(formid, createdate)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;