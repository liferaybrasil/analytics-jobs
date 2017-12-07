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