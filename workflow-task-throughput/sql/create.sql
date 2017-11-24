CREATE TABLE Analytics.WorkflowTaskAvg (
    kaleodefinitionversionid bigint,
    kaleotaskid bigint,
    classname text,
    classpk bigint,
    totalduration bigint,
    total bigint,
    PRIMARY KEY(kaleodefinitionversionid, kaleotaskid, classname, classpk)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;