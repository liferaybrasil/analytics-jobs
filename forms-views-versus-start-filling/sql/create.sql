CREATE TABLE Analytics.FormsAggregatedData (
	analyticsKey text,
	formId bigint,
	date timestamp,
	views bigint,
	sessions bigint,
	started bigint,
	converted bigint,
	convertedTotalTime bigint,
	dropoffs bigint,
	PRIMARY KEY(analyticsKey, formId, date)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;