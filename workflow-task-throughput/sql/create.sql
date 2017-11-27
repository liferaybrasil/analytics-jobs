CREATE TABLE Analytics.WorkflowTaskAvg (
    kaleodefinitionversionid bigint,    
    kaleotaskid bigint,
    totalduration bigint,
    total bigint,
    PRIMARY KEY(kaleotaskid)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;