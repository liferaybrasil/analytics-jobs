CREATE TABLE Analytics.FormsAggregatedData (
	analyticskey text,
	formid bigint,
	date date,
	views bigint,
	sessions bigint,
	started bigint,
	converted bigint,
	convertedtotaltime bigint,
	dropoffs bigint,
	PRIMARY KEY(analyticskey, formid, date)
)
WITH CLUSTERING ORDER BY (formid ASC, date ASC)
AND compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

CREATE TABLE Analytics.FormFieldsAggregatedData (
	analyticskey text,
	formid bigint,
	field text,
	date date,
	interactions bigint,
	totaltime double,
	empty bigint,
	refilled bigint,
	dropoffs bigint,
	PRIMARY KEY(analyticskey, formid, field, date)
)
WITH CLUSTERING ORDER BY (formid ASC, field ASC, date ASC)
AND compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;