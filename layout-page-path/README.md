Considering the follow input event table

```
 partitionkey    | createdate                      | applicationid | analyticskey | eventid | clientip | context                                           | eventproperties                                        | userid
-----------------+---------------------------------+---------------+--------------+---------+----------+---------------------------------------------------+--------------------------------------------------------+--------
 2017-11-22 09-3 | 2017-11-22 12:48:27.729000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-a'} |   {'referrer': 'http://localhost:8080/web/guest/home'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.764000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-a'} | {'referrer': 'http://localhost:8080/web/guest/page-c'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.767000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-c'} |   {'referrer': 'http://localhost:8080/web/guest/home'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.769000+0000 |    layout-7.0 |          XYZ |    view |     null |   {'url': 'http://localhost:8080/web/guest/home'} | {'referrer': 'http://localhost:8080/web/guest/page-b'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.771000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-a'} | {'referrer': 'http://localhost:8080/web/guest/page-c'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.773000+0000 |    layout-7.0 |          XYZ |    view |     null |   {'url': 'http://localhost:8080/web/guest/home'} | {'referrer': 'http://localhost:8080/web/guest/page-c'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.774000+0000 |    layout-7.0 |          XYZ |    view |     null |   {'url': 'http://localhost:8080/web/guest/home'} | {'referrer': 'http://localhost:8080/web/guest/page-a'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.776000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-a'} |   {'referrer': 'http://localhost:8080/web/guest/home'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.778000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-b'} |   {'referrer': 'http://localhost:8080/web/guest/home'} |   null
 2017-11-22 09-3 | 2017-11-22 12:48:27.780000+0000 |    layout-7.0 |          XYZ |    view |     null | {'url': 'http://localhost:8080/web/guest/page-b'} | {'referrer': 'http://localhost:8080/web/guest/page-a'} |   null
 ```
 After transformation data will be collapsed into 
 
```
destination                            | source                                 | total
----------------------------------------+----------------------------------------+-------
 http://localhost:8080/web/guest/page-a |   http://localhost:8080/web/guest/home |     2
 http://localhost:8080/web/guest/page-a | http://localhost:8080/web/guest/page-c |     2
   http://localhost:8080/web/guest/home | http://localhost:8080/web/guest/page-a |     1
   http://localhost:8080/web/guest/home | http://localhost:8080/web/guest/page-b |     1
   http://localhost:8080/web/guest/home | http://localhost:8080/web/guest/page-c |     1
 http://localhost:8080/web/guest/page-b |   http://localhost:8080/web/guest/home |     1
 http://localhost:8080/web/guest/page-b | http://localhost:8080/web/guest/page-a |     1
 http://localhost:8080/web/guest/page-c |   http://localhost:8080/web/guest/home |     1
 ```
 
 Where total is the number of users passing thru this path. For viz we might use the following query to create the journey
 
 Total users reaching destination: 
 
 `select sum(total) from Analytics.PagePaths where destination = 'http://localhost:8080/web/guest/page-a';`
 
 Sepecific path
 
`select source, total from Analytics.PagePaths where destination = 'http://localhost:8080/web/guest/page-a';`
