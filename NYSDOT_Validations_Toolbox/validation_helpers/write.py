import arcpy

import validation_helpers.utils as utils

from validation_helpers.active_routes import ACTIVE_ROUTES_QUERY


def roadway_level_attribute_result_to_reviewer_table(result_dict, versioned_layer, reviewer_ws,
                                                    reviewer_session, origin_table, base_where_clause=None,
                                                    level='info', logger=None, arcpy_messages=None):
    """
    This function takes a default dictionary of the violations identified in the
    `validate_by_roadway_type` function and loops through the keys and items of the default dict.
    The defaultdict keys are the violated rules' descriptions, and the values are a list of the ROUTE_IDs
    that violate the rule. The function selects the identified routes in the versioned milepoint layer,
    and writes the results to the reviewer table using the Milepoint geometry.

    Arguments
    ---------
    :param result_dict: A default dictionary from the collections library that contains the validation
        violations that were identified in `run_roadway_level_attribute_checks`
    :param versioned_layer: An arcpy feature layer that points to the correct database version
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param reviewer_session: The full reviewer session name
    :param origin_table: The table that contains the violation, which will be committed to the Reviewer Table

    Keyword Arguments
    -----------------
    :param base_where_clause: An ArcGIS where_clause that limits the results selection. Typically used to pass
        the where_clause that identifies the edited data into this function, so that only relevant violations
        are committed to the Reviewer Table
    :param level: A str that identifies the log level. Passed to the `log_it` function
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param arcpy_messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: Returns True when successful.
    """
    for rule_rids in result_dict.items():
        check_description = rule_rids[0]
        route_ids = rule_rids[1]
        if not route_ids:
            continue

        # Some checks currently returns 20000+ ROUTE_IDs, which results in an error using the
        #  ROUTE_ID IN () style query. Force a different where_clause for these cases to support
        #  the full_db_flag
        if check_description == 'SIGNING must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'SIGNING IS NULL AND ROADWAY_TYPE IN (1, 2) AND ({})'.format(ACTIVE_ROUTES_QUERY)

        elif check_description == 'ROUTE_SUFFIX must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'ROUTE_SUFFIX IS NOT NULL AND ROADWAY_TYPE IN (1, 2) AND ({})'.format(ACTIVE_ROUTES_QUERY)

        elif check_description == 'ROADWAY_FEATURE must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'ROADWAY_FEATURE IS NOT NULL AND ROADWAY_TYPE IN (1, 2) AND {()}'.format(ACTIVE_ROUTES_QUERY)

        elif check_description == 'ROUTE_QUALIFIER must be \'No Qualifier\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = (
                '(ROUTE_QUALIFIER <> 10 OR ROUTE_QUALIFIER IS NULL) AND ' +
                'ROADWAY_TYPE IN (1, 2) AND ({})'.format(ACTIVE_ROUTES_QUERY)
            )

        elif check_description == 'PARKWAY_FLAG must be \'No\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'PARKWAY_FLAG = \'T\' AND ROADWAY_TYPE IN (1, 2) AND ({})'.format(ACTIVE_ROUTES_QUERY)

        else:
            where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "') AND ({})".format(ACTIVE_ROUTES_QUERY)

        if base_where_clause:
            violations_where_clause = '({base_where}) AND ({validation_where})'.format(
                base_where=base_where_clause,
                validation_where=where_clause
            )
        else:
            violations_where_clause = where_clause


        utils.log_it(
            '{}: roadway_level_attribute_result where_clause={}'.format(check_description, violations_where_clause),
            level='info', logger=logger, arcpy_messages=arcpy_messages)

        arcpy.SelectLayerByAttribute_management(
            versioned_layer,
            'NEW_SELECTION',
            where_clause=violations_where_clause
        )

        in_memory_fc = utils.to_in_memory_fc(versioned_layer)

        utils.log_it('Calling WriteToReviewerTable_Reviewer geoprocessing tool',
            level='debug', logger=logger, arcpy_messages=arcpy_messages)

        arcpy.WriteToReviewerTable_Reviewer(
            reviewer_ws,
            reviewer_session,
            in_memory_fc,
            'ORIG_OBJECTID',
            origin_table,
            check_description
        )
        utils.log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)
        try:
            arcpy.Delete_management(in_memory_fc)
        except:
            pass
    return True

def co_dir_sql_result_to_reviewer_table(result_list, versioned_layer, reviewer_ws,
                                        reviewer_session, origin_table, check_description,
                                        dot_id_index=0, county_order_index=1,
                                        log_name='', level='info',
                                        logger=None, arcpy_messages=None):
    """
    This function is used to commit the results of the COUNTY_ORDER/DIRECTION SQL query to
    the Data Reviewer tables. The main input is the result list, which is a list of tuples
    returned from the arcpy.ArcSDESQLExecute method.

    The function uses the DOT_ID and COUNTY_ORDER that are returned from the SQL Queries to
    identify which ROUTE_IDs have violated the validation rule. Those features are then
    committed to the reviewer table.

    Arguments
    ---------
    :param result_list: A list of tuples or a list of lists that contains the COUNTY_ORDER and
        DIRECTION of the routes that violate the rule(s).
    :param versioned_layer: An arcpy feature layer that points to the correct database version
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param reviewer_session: The full reviewer session name
    :param origin_table: The table that contains the violation, which will be committed to the Reviewer Table
    :param check_description: A str of text that will be written to the description column of the
        Reviewer Table. This is typically the validation rule text

    Keyword Arguments
    -----------------
    :param dot_id_index: Defaults to 0. The index position of the dot_id in the `result_list`
    :param county_order_index: Defaults to 1. The index position of the county_order in the `result_list`
    :param log_name: Defaults to an empty string. The desired filepath for the log file
    :param level: Defaults to 'info'. The string identifying the log level, passed to the `utils.log_it` function
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param arcpy_messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: Returns True when successful.
    """
    dot_ids = [result_row[dot_id_index] for result_row in result_list]
    county_orders = [result_row[county_order_index] for result_row in result_list]

    where_clause = ''
    for dot_id, county_order in zip(dot_ids, county_orders):
        where_clause += 'DOT_ID = \'{dot_id}\' AND COUNTY_ORDER = \'{county_order}\' AND ({active_routes}) OR '.format(
            dot_id=dot_id,
            county_order=county_order,
            active_routes=ACTIVE_ROUTES_QUERY
        )
    # Remove the extra ' OR ' from the where_clause from the last iteration
    where_clause = where_clause[:-4]
    utils.log_it('{}: SQL Results where_clause: {}'.format(log_name, where_clause),
        level='info', logger=logger, arcpy_messages=arcpy_messages)

    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=where_clause
    )

    in_memory_fc = utils.to_in_memory_fc(versioned_layer)

    utils.log_it('Calling WriteToReviewerTable_Reviewer geoprocessing tool',
        level='debug', logger=logger, arcpy_messages=arcpy_messages)

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        reviewer_session,
        in_memory_fc,
        'ORIG_OBJECTID',
        origin_table,
        check_description
    )
    utils.log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)
    return True

def rdwy_attrs_sql_result_to_reviewer_table(result_list, versioned_layer, reviewer_ws,
                                            reviewer_session, origin_table, check_description,
                                            dot_id_index=0, log_name='', level='info',
                                            logger=None, arcpy_messages=None):
    """
    This function is used to commit the results of the roadway level attribute SQL query that
    ensures there is only one combination of the roadway level attributes per DOT_ID/COUNTY_ORDER.
    The main input is the result list, which is a list of tuples returned from the
    arcpy.ArcSDESQLExecute method.

    The function uses a pretty crazy bit of Python code to determine the ROUTE_IDs of the features that
    violate this query. The features are then selected in the `versioned_layer` and committed to
    the Data Reviewer Table.

    Arguments
    ---------
    :param result_list: A list of tuples or a list of lists that contains the COUNTY_ORDER and
        COUNT of the routes that violate the rule(s).
    :param versioned_layer: An arcpy feature layer that points to the correct database version
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param reviewer_session: The full reviewer session name
    :param origin_table: The table that contains the violation, which will be committed to the Reviewer Table
    :param check_description: A str of text that will be written to the description column of the
        Reviewer Table. This is typically the validation rule text

    Keyword Arguments
    -----------------
    :param dot_id_index: Defaults to 0. The index position of the DOT_ID in the `result_list`
    :param log_name: Defaults to an empty string. The desired filepath for the log file
    :param level: Defaults to 'info'. The string identifying the log level, passed to the `utils.log_it` function
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param arcpy_messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: Returns True when successful
    """
    # Get the DOT_IDs out of the SQL query results, and store them as a list
    dot_ids = [result_row[dot_id_index] for result_row in result_list]
    # Account for the rare case where a DOT_ID does not exist, which will be caught in another validation
    dot_ids = ['' if dot_id is None else dot_id for dot_id in dot_ids]

    # Select the DOT_IDs identified above on the active route data
    where_clause = "DOT_ID IN ('" + "', '".join(dot_ids) + "') AND ({})".format(ACTIVE_ROUTES_QUERY)
    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=where_clause
    )

    # Store the attributes that are used in the SQL query in a dictionary, with the
    #  ROUTE_ID as the key, and a list of the attributes as the value
    fields = [
        'ROUTE_ID', 'SIGNING', 'ROUTE_NUMBER', 'ROUTE_SUFFIX',
        'ROADWAY_TYPE', 'ROUTE_QUALIFIER', 'ROADWAY_FEATURE', 'PARKWAY_FLAG'
    ]
    with arcpy.da.SearchCursor(versioned_layer, fields) as curs:
        milepoint_attrs = {row[0]: list(row[1:]) for row in curs}

    # This bit of code gets a little wild. Since a DOT_ID can refer to a whole slew of routes, and
    #  there may be only a couple of those routes or even a single route that violate the
    #  validation, the following code widdles the results down to just the ROUTE_IDs of the violations
    #  It works by creating a list of all values stored in the milepoint_attrs dictionary, which is
    #  a list of lists. A list comprehension then further organizes those results by creating a separate
    #  list of lists, with the first element in the inner list being the count of the attribute occurrence
    #  in the milepoint_attrs.values, and the second element of the inner list being a list of the attributes
    #  themselves. A final list comprehension goes through the unique_attrs_occurrence_count list. If the
    #  first element of the inner list, which is the count of the attribute occurrence, is equal to 1,
    #  the index of those attributes in the milepoint_attrs.values dict is used to determine which key to
    #  select. The key is the ROUTE_ID, so this final list contains all ROUTE_IDs which violate the validation.
    milepoint_attr_values = milepoint_attrs.values()
    unique_attrs_occurrence_count = [
        [milepoint_attr_values.count(row), row] for row in milepoint_attr_values
    ]
    route_ids = [
        milepoint_attrs.keys()[milepoint_attrs.values().index(row[1])] for row in unique_attrs_occurrence_count
        if row[0] == 1
    ]

    output_where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "') AND ({})".format(ACTIVE_ROUTES_QUERY)

    utils.log_it('{}: SQL Results where_clause: {}'.format(log_name, output_where_clause),
        level='info', logger=logger, arcpy_messages=arcpy_messages)

    if len(route_ids) == 0:
        utils.log_it('    0 SQL query violations were found. Exiting with success code.',
            level='info', logger=logger, arcpy_messages=arcpy_messages)
        return True

    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=output_where_clause
    )

    in_memory_fc = utils.to_in_memory_fc(versioned_layer)

    utils.log_it('Calling WriteToReviewerTable_Reviewer geoprocessing tool',
        level='debug', logger=logger, arcpy_messages=arcpy_messages)

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        reviewer_session,
        in_memory_fc,
        'ORIG_OBJECTID',
        origin_table,
        check_description
    )
    utils.log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)

    return True
