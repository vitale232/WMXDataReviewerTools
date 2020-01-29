"""
There are some basic configuration variables stored in this file. Variables that most likely will be consistent across
NYSDOT's system through the years are setup here in the event they one day change. Hopefully this will also 
have the affect of making this code more useful to anyone that stumbles upon it.

In addition to configuration, there are strings defined here. The Python string definitions
are all some variation of a SQL query.

IMPORTANT
---------
- If the variable name ends with "_QUERY", the variable is a SQL query that can be executed as is.
- If the variable name ends with "_QUERY_FMT", the variable is a Python string that is intended to be used with the
    `__str__.format()` method.

Format Example
--------------
>>> EDITED_ROUTES_QUERY_FMT.format(
>>>     date=datetime.now(),
>>>     user_upper='AVITALE',
>>>     user_lower='avitale',
>>>     domain=DOMAIN.lower(),
>>>     active_routes=ACTIVE_ROUTES_QUERY
>>> )
"EDITED_DATE >= '2020-01-28 14:31:22.435000' AND (EDITED_BY = 'AVITALE' OR EDITED_BY = 'avitale@svc') AND ((FROM_DATE 
IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP))"
"""


# Config Variables
DOMAIN = 'SVC'
LRSN_FC_WILDCARD = '*LRSN_Milepoint'
# TODO: Consider moving arcpy.da.cursor field lists to this file. For now, leave them in the code for readability

# SQL Queries and Where Clauses
ACTIVE_ROUTES_WHERE_CLAUSE = (
    '(FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)'
)

UNIQUE_RDWY_ATTRS_QUERY= (
    'SELECT DOT_ID, COUNTY_ORDER, ' +
        'COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ' +
        'ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG)) ' +
    'FROM ELRS.elrs.LRSN_Milepoint_evw ' +
    'WHERE ' +
        '(FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND ' +
        '(TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP) ' +
    'GROUP BY DOT_ID, COUNTY_ORDER ' +
    'HAVING COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ' +
    'ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))>1;'
)

UNIQUE_CO_DIR_QUERY = (
    'SELECT DOT_ID, COUNTY_ORDER, DIRECTION, COUNT (1) ' +
    'FROM ELRS.elrs.LRSN_Milepoint_evw ' +
    'WHERE ' +
        '(FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND ' +
        '(TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP) ' +
    'GROUP BY DOT_ID, COUNTY_ORDER, DIRECTION ' +
    'HAVING COUNT (1)>1;'
)


# SQL Query Formats
EDITED_ROUTES_QUERY_FMT = (
    'EDITED_DATE >= \'{date}\' AND ' +
    '(EDITED_BY = \'{user_upper}\' OR EDITED_BY = \'{user_lower}@{domain}\') AND ({active_routes})'
)
