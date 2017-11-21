CREATE TABLE Analytics.WorkflowEntities (
    entity TEXT,
    id bigint,
    name TEXT,
    PRIMARY KEY(entity, id)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;