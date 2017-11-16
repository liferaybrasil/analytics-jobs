CREATE TABLE Analytics.WorkflowProcessAvg (
    kaleodefinitionversionid bigint,
    totalduration bigint,
    total bigint,
    PRIMARY KEY(kaleodefinitionversionid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;