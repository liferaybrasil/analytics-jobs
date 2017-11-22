CREATE TABLE Analytics.PagePaths (
    source VARCHAR,
    destination VARCHAR,
    total BIGINT,
    PRIMARY KEY(destination, source, total)
)
WITH compaction = {'class': 'DateTieredCompactionStrategy'}
AND default_time_to_live = 7776000;

//
// Query Example to be used for navigation
//

select source, total from Analytics.PagePaths where destination = 'http://localhost:8080/web/guest/page-a';