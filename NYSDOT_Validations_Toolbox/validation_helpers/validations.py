from collections import defaultdict
import datetime
import logging
import re
import time
import traceback

import arcpy

import validation_helpers.utils as utils
import validation_helpers.write as write

from validation_helpers.active_routes import ACTIVE_ROUTES_QUERY


def run_batch_on_buffered_edits(reviewer_ws, batch_job_file,
                                production_ws, job__id,
                                job__started_date, job__owned_by,
                                version_milepoint_layer=None,
                                milepoint_fc=None,
                                full_db_flag=False,
                                logger=None, messages=None):
    """
    This function executes the Data Reviewer Batch Job. The workflow of the function is as follows:
    1. Use the WMX job__id and job__owned by "tokens" to capture the version name and username
    2. Use the full_db_flag or job__owned_by and job__started_date WMX token to figure out which features to validate.
       If the full_db_flag is set, all features are validated. If the full_db_flag is False, the job__owned_by and
       job__started_date parameters are used to query the LRSN_Milepoint.EDITED_BY and LRSN_Milepoint.EDITED_DATE
       fields for recent updates. EDITED_BY and EDITED_DATE are automatically updated with the username and time
       of the transaction for edits of existing data and creation of new data
    3. Run the Data Reviewer Batch Job using the Geoprocessing tool with the buffer polygons or LRSN_Milepoint
       extent as the area of interst (which is used is determined by the full_db_flag)

    Arguments
    ---------
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param batch_job_file: Filepath to the Reviewer Batch Job file (.rbj) that was created using ArcGIS Desktop
    :param production_ws: Filepath to the SDE file pointing to the correct database. The version of the SDE filepath
        does not matter as long as it exists. The function will change to the user's version.
    :param job__id: The integer value from the WMX [JOB:ID] token. Can be set manually if run via ArcMap
    :param job__started_date: The date value from the WMX [JOB:STARTED_DATE] token. Can be set manually
        if run via ArcMap.
    :param job__owned_by: The username from the WMX [JOB:OWNED_BY] token. Can be set manually if run
        via ArcMap.

    Keyword Arguments
    -----------------
    :param full_db_flag: Defaults to False. If True, all features will be validated. If False, only
        features edited by the user in their version will be validated.
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: When successful, the function returns True
    """
    try:
        if not logger:
            logger = utils.initialize_logger(log_path=None, log_level=logging.INFO)

        arcpy.CheckOutExtension('datareviewer')

        # Set the database connection as the workspace. All table and FC references come from here
        arcpy.env.workspace = production_ws

        user, production_ws_version = utils.get_user_and_version(
            job__owned_by,
            job__id,
            production_ws,
            logger=None,
            arcpy_messages=None
        )

        if not full_db_flag:
            utils.log_it(('Calling run_batch_on_buffered_edits(): Selecting edits made by {} '.format(user) +
                    'in {} and buffering by 10 meters'.format(production_ws_version)),
                    level='info', logger=logger, arcpy_messages=messages)
        else:
            utils.log_it(('Calling run_batch_on_buffered_edits(): Running Reviewer Batch Job on ' +
                'all features in {}'.format(production_ws_version)),
                level='info', logger=logger, arcpy_messages=messages)

        if not version_milepoint_layer:
            # Select the milepoint LRS feature class from the workspace if not passed into function
            milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
                production_ws,
                production_ws_version
            )

        if not full_db_flag:
            # Filter out edits made by this user on this version since its creation
            where_clause = 'EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\' AND ({active_routes})'.format(
                date=job__started_date,
                user=user.upper(),
                active_routes=ACTIVE_ROUTES_QUERY
            )
        else:
            where_clause = ACTIVE_ROUTES_QUERY
        utils.log_it('where_clause={}'.format(where_clause), logger=logger, arcpy_messages=messages)
        utils.log_it('Using where_clause to find recent edits of {}: {}'.format(milepoint_fc, where_clause),
            level='debug', logger=logger, arcpy_messages=messages)

        version_select_milepoint_layer = arcpy.SelectLayerByAttribute_management(
            version_milepoint_layer,
            'NEW_SELECTION',
            where_clause=where_clause
        )

        if not full_db_flag:
            # Since the full_db_flag is False, let's find the features that were edited
            #  by this user in this version since it was created. We'll then buffer those
            #  features by a generous 10 meters, and validate all features that lie
            #  within the polygons using a RBJ
            feature_count = int(arcpy.GetCount_management(version_select_milepoint_layer).getOutput(0))
            if feature_count == 0:
                utils.log_it(('0 features were identified as edited since {}. '.format(job__started_date) +
                    'Exiting without running validations!'),
                    level='warn', logger=logger, arcpy_messages=messages)
                return False

            utils.log_it('{count} features selected'.format(count=feature_count),
                level='debug', logger=logger, arcpy_messages=messages)

            utils.log_it('Buffering edited routes by 10 meters',
                level='info', logger=logger, arcpy_messages=messages)
            area_of_interest = 'in_memory\\mpbuff_{}'.format(int(time.time()))

            # Set the output coordinate reference for the Buffer_analysis call
            arcpy.env.outputCoordinateSystem = arcpy.Describe(milepoint_fc).spatialReference

            arcpy.Buffer_analysis(
                version_select_milepoint_layer,
                area_of_interest,
                '10 Meters',
                dissolve_option='ALL'
            )
            utils.log_it('', level='gp', logger=logger, arcpy_messages=messages)
        else:
            area_of_interest = arcpy.Describe(version_milepoint_layer).extent

        # Data Reviewer WMX tokens are only supported in the default DR Step Types. We must back out
        #  the session name from the DR tables
        reviewer_session = utils.get_reviewer_session_name(
            reviewer_ws,
            job__owned_by,
            job__id,
            logger=logger,
            arcpy_messages=messages
        )

        utils.log_it('Calling ExecuteReviewerBatchJob_Reviewer geoprocessing tool',
            level='debug', logger=logger, arcpy_messages=messages)

        reviewer_results = arcpy.ExecuteReviewerBatchJob_Reviewer(
            reviewer_ws,
            reviewer_session,
            batch_job_file,
            production_workspace=production_ws,
            analysis_area=area_of_interest,
            changed_features='ALL_FEATURES',
            production_workspaceversion=production_ws_version
        )
        utils.log_it('', level='gp', logger=logger, arcpy_messages=messages)

        try:
            # Try to cleanup the runtime environment
            arcpy.CheckInExtension('datareviewer')
            arcpy.env.workspace = r'in_memory'
            fcs = arcpy.ListFeatureClasses()
            utils.log_it('in_memory fcs: {}'.format(fcs), level='debug', logger=logger, arcpy_messages=messages)
            for in_memory_fc in fcs:
                try:
                    arcpy.Delete_management(in_memory_fc)
                    utils.log_it(' Deleted: {}'.format(in_memory_fc),
                        level='debug', logger=logger, arcpy_messages=messages)
                except:
                    utils.log_it(' Could not delete: {}'.format(in_memory_fc),
                        level='debug', logger=logger, arcpy_messages=messages)
                    pass
        except Exception as exc:
            utils.log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
            pass

        utils.log_it('GP Tool success at: {}'.format(datetime.datetime.now()),
            level='info', logger=logger, arcpy_messages=messages)

    except Exception as exc:
        utils.log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
        raise exc
    
    # Return True on function success
    else:
        return True

def run_sql_validations(reviewer_ws, production_ws, job__id,
                        job__started_date, job__owned_by,
                        version_milepoint_layer=None,
                        milepoint_fc=None,
                        logger=None, messages=None):
    """
    This tool executes two SQL queries against the SDE versioned view of the LRSN_Milepoint table.
    By using the versioned view, we ensure that all new features are validated, and we get the added
    bonus of validating the entire table.

    The two queries introspect on signed routes (and 900 administrative routes) and divided roadways.
    One query looks at numerous attributes, including SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE,
    ROUTE_QUALIFIER, ROADWAY_FEATURE, and PARKWAY_FLAG, and ensures that there is one unique combination
    of those features across a DOT_ID and COUNTY_ORDER combination. For example, I-90 in Albany County
    should have only one attribute roadway level attribute that distinguishes between the primary and
    non-primary R&H routes, which is the DIRECTION attribute. If I-90 primary has a ROUTE_SUFFIX=None
    but I-90 reverse has a ROUTE_SUFFIX=NULL, this validation will be violated. The ROUTE_IDs of the offending
    DOT_ID/COUNTY_ORDER combination are committed to the Reviewer Table. The second query ensures that
    the DIRECTION code is not repeated across a DOT_ID/COUNTY_ORDER combination. For example, if both I-90
    primary and reverse were set to have a DIRECTION code of 0, this validation will commit them to the
    reviewer table.

    This function does the following:
    1. Establishes a connection to the database using the `arcpy.ArcSDESQLExecute` class, which allows the
       execution of arbitrary SQL code (this gets around the typical ArcGIS where_clause pattern)
    2. Parses the query responses for violations. If there are violations, the violating routes are
       written to the Reviewer Table

    Arguments
    ---------
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param production_ws: Filepath to the SDE file pointing to the correct database. The version of the SDE filepath
        does not matter as long as it exists. The function will change to the user's version.
    :param job__id: The integer value from the WMX [JOB:ID] token. Can be set manually if run via ArcMap
    :param job__started_date: The date value from the WMX [JOB:STARTED_DATE] token. Can be set manually
        if run via ArcMap.
    :param job__owned_by: The username from the WMX [JOB:OWNED_BY] token. Can be set manually if run
        via ArcMap.

    Keyword Arguments
    -----------------
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: When successful, the function returns True
    """
    try:
        # These are the sql queries that need to be run against the full Milepoint table.
        arcpy.CheckOutExtension('datareviewer')

        unique_rdwy_attrs_sql= (
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

        unique_co_dir_sql = (
            'SELECT DOT_ID, COUNTY_ORDER, DIRECTION, COUNT (1) ' +
            'FROM ELRS.elrs.LRSN_Milepoint_evw ' +
            'WHERE ' +
                '(FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND ' +
                '(TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP) ' +
            'GROUP BY DOT_ID, COUNTY_ORDER, DIRECTION ' +
            'HAVING COUNT (1)>1;'
        )

        utils.log_it(('Calling run_sql_validations(): Running SQL queries against the ' +
            'ELRS in HDS_GENERAL_EDITING_JOB_{} and writing '.format(job__id)) +
            'results to Data Reviewer session',
                level='info', logger=logger, arcpy_messages=messages)

        arcpy.env.workspace = production_ws

        user, production_ws_version = utils.get_user_and_version(
            job__owned_by,
            job__id,
            production_ws,
            logger=None,
            arcpy_messages=None
        )

        utils.log_it(('Connecting to the production database and executing ' +
            'SQL validations -> database: {} | version: {}'.format(
                production_ws, production_ws_version
            )), level='info', logger=logger, arcpy_messages=messages)

        connection = arcpy.ArcSDESQLExecute(production_ws)
        # Change the SDE versioned view to the Workflow Manager version
        change_versioned_view_sql = 'EXEC ELRS.sde.set_current_version \'{version_name}\';'.format(
            version_name=production_ws_version
        )
        utils.log_it('Calling SQL Query on database connection: "{sql}"'.format(sql=change_versioned_view_sql),
            level='debug', logger=logger, arcpy_messages=messages)
        connection.execute(change_versioned_view_sql)

        unique_rdwy_attrs_result = connection.execute(unique_rdwy_attrs_sql)
        utils.log_it('Calling SQL Query on database connection: "{sql}"'.format(sql=unique_rdwy_attrs_sql),
            level='debug', logger=logger, arcpy_messages=messages)

        unique_co_dir_result = connection.execute(unique_co_dir_sql)
        utils.log_it('Calling SQL Query on database connection: "{sql}"'.format(sql=unique_co_dir_sql),
            level='debug', logger=logger, arcpy_messages=messages)

        # Try changing the connection/versioned view back to Lockroot to release locks on the edit version for WMX
        connection.execute('EXEC ELRS.sde.set_current_version "ELRS.Lockroot";')

        # If the query succeeds but the response is empty, arcpy returns a python
        #  boolean type with a True value. Convert booleans to an empty list to
        #  simplify the results handling logic.
        if isinstance(unique_co_dir_result, bool):
            unique_co_dir_result = []
        if isinstance(unique_rdwy_attrs_result, bool):
            unique_rdwy_attrs_result = []

        if len(unique_rdwy_attrs_result) > 0 or len(unique_co_dir_result) > 0:
            # Create a versioned arcpy feature layer of the Milepoint feature class
            if not milepoint_fc:
                milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
                if len(milepoint_fcs) == 1:
                    milepoint_fc = milepoint_fcs[0]
                else:
                    raise ValueError(
                        'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
                        'Selected FCs: {}'.format(milepoint_fcs)
                    )

            if not version_milepoint_layer:
                # Select the milepoint LRS feature class from the workspace if not passed into function
                utils.log_it('Creating versioned view of {}'.format(milepoint_fc),
                    level='debug', logger=logger, arcpy_messages=messages)

                milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
                    production_ws,
                    production_ws_version
                )
                utils.log_it('version_milepoint_layer created... This could lead to a WMX Version lock! : {}'.format(
                    version_milepoint_layer), level='warn', logger=logger, arcpy_messages=messages)

            reviewer_session_name = utils.get_reviewer_session_name(
                reviewer_ws,
                job__owned_by,
                job__id,
                logger=logger,
                arcpy_messages=messages
            )

            if len(unique_rdwy_attrs_result) > 0:
                unique_rdwy_attrs_check_title = 'ROUTE_ID with improper roadway-level attributes across DOT_ID'

                write.rdwy_attrs_sql_result_to_reviewer_table(
                    unique_rdwy_attrs_result,
                    version_milepoint_layer,
                    reviewer_ws,
                    reviewer_session_name,
                    milepoint_fc,
                    unique_rdwy_attrs_check_title,
                    level='debug',
                    logger=logger,
                    arcpy_messages=messages,
                    log_name='unique_rdwy_attrs_check'
                )
            if len(unique_co_dir_result) > 0:
                unique_co_dir_check_title = 'Non-Unique COUNTY_ORDER and DIRECTION for this DOT_ID'

                write.co_dir_sql_result_to_reviewer_table(
                    unique_co_dir_result,
                    version_milepoint_layer,
                    reviewer_ws,
                    reviewer_session_name,
                    milepoint_fc,
                    unique_co_dir_check_title,
                    level='debug',
                    logger=logger,
                    arcpy_messages=messages,
                    log_name='unique_co_dir_check'
                )

        try:
            # Try to cleanup the runtime environment
            arcpy.CheckInExtension('datareviewer')
            try:
                del connection
            except Exception:
                utils.log_it('validations.run_sql_validations(): Could not delete the database connection!',
                    level='warn', logger=logger, arcpy_messages=messages)
            arcpy.env.workspace = r'in_memory'
            fcs = arcpy.ListFeatureClasses()
            utils.log_it('in_memory fcs: {}'.format(fcs), level='debug', logger=logger, arcpy_messages=messages)
            for in_memory_fc in fcs:
                try:
                    arcpy.Delete_management(in_memory_fc)
                    utils.log_it(' Deleted: {}'.format(in_memory_fc),
                        level='debug', logger=logger, arcpy_messages=messages)
                except:
                    utils.log_it(' Could not delete: {}'.format(in_memory_fc),
                        level='debug', logger=logger, arcpy_messages=messages)
                    pass
        except Exception as exc:
            utils.log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
            pass

        utils.log_it('GP Tool success at: {}'.format(datetime.datetime.now()),
            level='info', logger=logger, arcpy_messages=messages)

    except Exception as exc:
        utils.log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
        raise exc

    # Return True on function success
    else:
        return True

def run_roadway_level_attribute_checks(reviewer_ws, production_ws, job__id,
                                       job__started_date, job__owned_by,
                                       version_milepoint_layer=None,
                                       milepoint_fc=None,
                                       full_db_flag=False,
                                       logger=None, messages=None):
    """
    This function manages the execution of the "Roadway level attribute" validations on the
    LRSN_Milepoint table. Roadway level attributes refers to the business data stored in
    the LRSN_Milepoint table, that applies to the entirety of the route. Some examples
    include SIGNING, COUNTY, and ROUTE_NUMBER.

    All of the validations are based on the ROADWAY_TYPE (which is also a roadway level attribute).
    This means that ROADWAY_TYPE itself cannot be validated, so it is up to the
    user to ensure at least this field is correct.

    This function executes by:
    1. Selecting the user edited routes from the current version, or selecting all active routes
       from the table (depending on the full_db_flag value)
    2. Iterates through the selected features attributes, and calls the `validate_by_roadway_type`
       function on each individual feature
    3. Writes the output to the Reviewer table.

    Arguments
    ---------
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param production_ws: Filepath to the SDE file pointing to the correct database. The version of the SDE filepath
        does not matter as long as it exists. The function will change to the user's version.
    :param job__id: The integer value from the WMX [JOB:ID] token. Can be set manually if run via ArcMap
    :param job__started_date: The date value from the WMX [JOB:STARTED_DATE] token. Can be set manually
        if run via ArcMap.
    :param job__owned_by: The username from the WMX [JOB:OWNED_BY] token. Can be set manually if run
        via ArcMap.

    Keyword Arguments
    -----------------
    :param full_db_flag: Defaults to False. If True, all features will be validated. If False, only
        features edited by the user in their version will be validated.
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: When successful, the function returns True
    """
    try:
        if not logger:
            logger = utils.initialize_logger(log_path=None, log_level=logging.INFO)

        utils.log_it(
            'Calling run_roadway_level_attribute_checks(): Selecting newly created ' +
            'or edited routes by this user in this version and validating their attributes',
            level='info', logger=logger, arcpy_messages=messages)

        arcpy.CheckOutExtension('datareviewer')

        # Set the database connection as the workspace. All table and FC references come from here
        arcpy.env.workspace = production_ws

        user, production_ws_version = utils.get_user_and_version(
            job__owned_by,
            job__id,
            production_ws,
            logger=logger,
            arcpy_messages=messages
        )

        if not version_milepoint_layer:
            # Get a versioned view of milepoint if not passed in
            milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
                production_ws,
                production_ws_version
            )

        # Since the search cursor on the feature layer is affected by previous where_clause's, let's grab all of
        #  the dot_ids and route_id:direction combos before the where clause is applied. This gets passed into 
        #  validate_county_order_value while looping through the validation features
        dot_id_routes = dict()
        fields = ['DOT_ID', 'COUNTY_ORDER', 'ROUTE_ID', 'DIRECTION']
        with arcpy.da.SearchCursor(version_milepoint_layer, fields, where_clause=ACTIVE_ROUTES_QUERY) as id_cursor:
            for row in id_cursor:
                dot_id, county_order, route_id, direction = row
                try:
                    if county_order is None:
                        raise ValueError
                    to_int = int(county_order)
                    dot_id_routes[dot_id][int(county_order)].append(route_id + ':' + direction)
                except KeyError:
                    dot_id_routes[dot_id] = defaultdict(list)
                    dot_id_routes[dot_id][(int(county_order))].append(route_id + ':' + direction)
                except ValueError:
                    pass

        # If the full_db_flag is True, run the validations on all active routes (routes with no TO_DATE).
        #  Otherwise, follow the typical pattern of selecting the data edited by this user
        #  in this version since it was created.
        if full_db_flag:
            where_clause = ACTIVE_ROUTES_QUERY
        else:
            where_clause = 'EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\' AND ({active_routes})'.format(
                date=job__started_date,
                user=user.upper(),
                active_routes=ACTIVE_ROUTES_QUERY
            )
        utils.log_it('Using where_clause to find recent edits of {}: {}'.format(milepoint_fc, where_clause),
            level='debug', logger=logger, arcpy_messages=messages)

        version_select_milepoint_layer = arcpy.SelectLayerByAttribute_management(
            version_milepoint_layer,
            'NEW_SELECTION',
            where_clause=where_clause
        )
        utils.log_it('{count} features selected'.format(
            count=arcpy.GetCount_management(version_select_milepoint_layer).getOutput(0)),
            level='debug', logger=logger, arcpy_messages=messages)

        # If these fields in precisely this order are not passed to the SearchCursor,
        #  the validate_roadway_type function will fail. Any updates to either function
        #  should be careful with the field orders.
        attribute_fields = [
            'ROADWAY_TYPE', 'ROUTE_ID', 'DOT_ID', 'COUNTY_ORDER', 'SIGNING', 'ROUTE_NUMBER',
            'ROUTE_SUFFIX', 'ROUTE_QUALIFIER', 'PARKWAY_FLAG', 'ROADWAY_FEATURE', 'DIRECTION',
        ]
        violations = defaultdict(list)
        with arcpy.da.SearchCursor(version_select_milepoint_layer, attribute_fields) as curs:
            for row in curs:
                # Validate roadway level attributes
                # Don't include ROADWAY_TYPE or DIRECTION in the attributes variable
                roadway_type = row[0]
                attributes = row[1:-1]

                results = validate_by_roadway_type(roadway_type, attributes)
                for rule_rids in results.items():
                    violations[rule_rids[0]].append(*rule_rids[1])

                # Validate the COUNTY_ORDER range for this DOT_ID
                dot_id = row[2]
                county_order = row[3]
                try:
                    county_order_int = int(county_order)
                except ValueError:
                    # If the county_order is not integer like (e.g. '01'), it will be caught in validate_by_roadway_type
                    pass
                except TypeError:
                    # If there  is no county_order, it will be caught in validate_by_roadway_type
                    pass
                else:
                    direction = row[-1]
                    county_order_results = validate_county_order_value(
                        dot_id_routes,
                        dot_id,
                        direction,
                        logger=logger,
                        arcpy_messages=messages
                    )
                    for violation_desc__rid in county_order_results.items():
                        violations[violation_desc__rid[0]].extend(violation_desc__rid[1])

        if len(violations) == 0:
            utils.log_it('  0 roadway level attribute violations found. Exiting with success code',
                level='warn', logger=logger, arcpy_messages=messages)
            return True

        session_name = utils.get_reviewer_session_name(
            reviewer_ws,
            job__owned_by,
            job__id,
            logger=logger,
            arcpy_messages=messages
        )

        write.roadway_level_attribute_result_to_reviewer_table(
            violations,
            version_milepoint_layer,
            reviewer_ws,
            session_name,
            milepoint_fc,
            base_where_clause=where_clause,
            level='info',
            logger=logger,
            arcpy_messages=messages
        )
        try:
            # Try to cleanup the runtime environment
            arcpy.CheckInExtension('datareviewer')
            arcpy.env.workspace = r'in_memory'
            fcs = arcpy.ListFeatureClasses()
            utils.log_it('in_memory fcs: {}'.format(fcs), level='debug', logger=logger, arcpy_messages=messages)
            for in_memory_fc in fcs:
                try:
                    arcpy.Delete_management(in_memory_fc)
                    utils.log_it(' Deleted: {}'.format(in_memory_fc),
                        level='debug', logger=logger, arcpy_messages=messages)
                except:
                    utils.log_it(' Could not delete: {}'.format(in_memory_fc),
                        level='debug', logger=logger, arcpy_messages=messages)
                    pass
        except Exception as exc:
            utils.log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
            pass

        utils.log_it('GP Tool success at: {}'.format(datetime.datetime.now()),
            level='info', logger=logger, arcpy_messages=messages)

    except Exception as exc:
        utils.log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
        raise exc

    # Return True on function success
    else:
        return True

def validate_county_order_value(dot_id_routes, dot_id, direction,
                                active_routes_where_clause=ACTIVE_ROUTES_QUERY,
                                logger=None, arcpy_messages=None):
    """
    This function ensures that COUNTY_ORDER values are valid for the validated route's DOT_ID.
    Valid county orders should increment by a value of 1 as they traverse county boundaries, starting with a
    COUNTY_ORDER of 01. If a DOT_ID crosses three counties, the expected COUNTY_ORDER sequence is
    ('01, '02', '03').

    Arguments
    ---------
    :param dot_id_routes: A dictionary containing defaultdicts of lists. The dict key is the DOT_ID, the defaultdict
        key is the COUNTY_ORDER (as an integer), and the list is a list of ROUTE_IDs
    :param dot_id: A string value of length 6 that identifies a unique roadway
    :param direction: A string value of one of ('0', '1', '2', '3') that represents the route's direction
        significance. Each code has a meaning associated with divided roadways and where the inventory data is stored

    Keyword Arguments
    -----------------
    :param active_routes_where_clause: An ArcGIS where_clause that filters out retired data

    Returns
    -------
    :returns violations: A defaultdict of a list that has the violated rule as the key, and the list of violating
        route_ids as the values
    """
    violations = defaultdict(list)
    rule_text_sequence = 'COUNTY_ORDER does not increment by a value of 1 for this DOT_ID'
    rule_text_not_one = 'COUNTY_ORDER does not equal \'01\' for singular DOT_ID'
    rule_text_length_error = 'COUNTY_ORDER has too many ROUTE_IDs for this DOT_ID'

    county_orders = dot_id_routes[dot_id]

    # Since multiple ROUTE_IDs for a COUNTY_ORDER is valid (with certain direction code combinations), we need to
    #  test that the direction codes are valid
    for county_order, route_id_direction in county_orders.items():
        # Length of one means there is only one ROUTE_ID for this COUNTY_ORDER, so add it to the dict witout :DIRECTION
        if len(route_id_direction) == 1:
            county_orders[county_order] = [route_id_direction[0].split(':')[0]]
        else:
            direction_codes = [route_id_direction_str.split(':')[1] for route_id_direction_str in route_id_direction]
            if len(direction_codes) > 1:
                process_direction_codes = True
            else:
                # If there is only one ROUTE_ID for this DOT_ID/COUNTY_ORDER, 0 is the only valid direction code
                process_direction_codes = False
                valid_direction_codes = ['0']

            # If there is more than one ROUTE_ID for the DOT_ID/COUNTY_ORDER, there are two valid DIRECTION combos
            #  which are:
            #  0 - Prim. Dir. w Unvdivided Inventory AND 3 - Rev. Dir. w No Inventory
            #  1 - Prim. Dir. w Divided Inventory AND 2 - Rev. Dir. w Divided Inventory
            if process_direction_codes and '0' in direction_codes:
                valid_direction_codes = ['0', '3']
            if process_direction_codes and '1' in direction_codes:
                valid_direction_codes = ['1', '2']

            if all(valid_dir in direction_codes for valid_dir in valid_direction_codes):
                # The directions are valid, so just store one ROUTE_ID in the list for this COUNTY_ORDER. This makes
                #  the logic below simpler, since we can assume only one ROUTE_ID exists for each COUNTY_ORDER
                county_orders[county_order] = [ route_id_direction[0].split(':')[0] ]
            else:
                # The case where DIRECTION are not valid should be covered by the SQL validations TODO: Check this
                pass
        if len(route_id_direction) > 2:
            # There should not be more than 2 ROUTE_IDs per DOT_ID/COUNTY_ORDER. Write just one ROUTE_ID as a violation
            violations[rule_text_length_error] = [ rid_dir.split(':')[0] for rid_dir in route_id_direction ]

    
    route_ids = [route_id for all_route_ids in county_orders.values() for route_id in all_route_ids]
    expected_county_orders = range(1, len(route_ids)+1)

    if sorted(county_orders.keys()) == sorted(expected_county_orders):
        # The county orders increment correctly, so pass to function return
        pass
    else:
        if len(county_orders.keys()) == 1:
            if county_orders.keys()[0] != 1:
                # There is only one COUNTY_ORDER, and it was not 01. Store the violation
                for route_id_dirs in county_orders.values():
                    violation_route_ids = [route_id.split(':')[0] for route_id in route_id_dirs]
                    violations[rule_text_not_one] = violation_route_ids
        else:
            # There are numerous county orders. Test that element 1 minus element 0 == 1
            #  For example: ['01', '03'][1] - ['01', '03'][0] -> 3-1 != 1
            for county_order_a, county_order_b in zip(county_orders.keys()[:-1], county_orders.keys()[1:]):
                difference = county_order_b - county_order_a
                if difference == 1:
                    continue
                else:
                    # If not equal to 1, write a violation
                    violations[rule_text_sequence].append(*county_orders[county_order_b])

    # Make sure ROUTE_IDs are unique if they exist
    # if violations:
    #     violations[rule_text] = list(set(violations[rule_text]))
    if violations:
        for rule in violations.keys():
            violations[rule] = list(set(violations[rule]))

    return violations


def validate_by_roadway_type(roadway_type, attributes):
    """
    This function validates a single feature based on its roadway type. The function is set up to
    accept the "coded values" of the domains rather than the "descriptions". Based on the particular
    ROADWAY_TYPE, the remaining roadway level attributes are passed through a series of if statements.
    If the statement is True, the attributes violate the validations. A new entry is added to the
    `violations` defaultdict, with the rule as the defaultdict key, and the values as a list of offending
    ROUTE_IDs

    The attribute fields must be in the following order:
    signing, route_number, route_suffix, route_qualifier, parkway_flag, and roadway_feature
    Otherwise these validations are invalid!!!!!

    Arguments
    ---------
    :param roadway_type: An int value that represents the coded value from the database for a route's
        ROADWAY_TYPE attribute. These values are typically entered into the feature class using a R&H tool,
        the SEE application, or an edit to the attribute table in ArcGIS
    :param attributes: A list of values that represent the roadway level attribute coded values from the
        database for the specific feature that is being validated. The attributes must be constructed
        in this order: [
            route_id, dot_id, county_order, signing, route_number,
            route_suffix, route_qualifier, parkway_flag, roadway_feature
        ]

    Returns
    -------
    :returns defaultdict(list): This function returns a default dict that contains a list of the offending ROUTE_IDs
        as the dict items, and the rule(s) that was validated as the default dict keys.
    :raises AttributeError: Raises an AttributeError if the roadway_type is not within the valid range
    """
    (route_id, dot_id, county_order, signing, route_number,
        route_suffix, route_qualifier, parkway_flag, roadway_feature) = attributes
    if roadway_type not in [1, 2, 3, 4, 5]:
        raise AttributeError(
            'ROADWAY_TYPE is outside of the valid range. Must be one of (1, 2, 3, 4 ,5). ' +
            'Currently ROADWAY_TYPE={}'.format(roadway_type)
        )

    violations = defaultdict(list)

    # All validations regardless of ROADWAY_TYPE
    if not re.match(r'^\d{9}$', str(route_id)):
        violations['ROUTE_ID must be a nine digit number'].append(route_id)
    if not re.match(r'^\d{6}$', str(dot_id)):
        violations['DOT_ID must be a six digit number'].append(route_id)
    if not re.match(r'^\d{2}$', str(county_order)):
        violations['COUNTY_ORDER must be a zero padded two digit number (e.g. \'01\')'].append(route_id)

    # Only run these validations if the county order is a number
    if re.match(r'^\d{2}$', str(county_order)):
        if county_order and int(county_order) == 0:
            violations['COUNTY_ORDER must be greater than \'00\''].append(route_id)
        if county_order and int(county_order) > 28:
            violations['COUNTY_ORDER should be less than \'29\''].append(route_id)

    # Validations for ROADWAY_TYPE = Road or Ramp
    if roadway_type == 1 or roadway_type == 2:
        if signing:
            violations['SIGNING must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(route_id)
        if route_number:
            violations['ROUTE_NUMBER must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(route_id)
        if route_suffix:
            violations['ROUTE_SUFFIX must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(route_id)
        if route_qualifier != 10:    # 10 is "No Qualifier"
            violations[(
                'ROUTE_QUALIFIER must be \'No Qualifier\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')'
            )].append(route_id)
        if parkway_flag == 'T':      # T is "Yes"
            violations['PARKWAY_FLAG must be \'No\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(route_id)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(route_id)

    # Validations for ROADWAY_TYPE = Route
    elif roadway_type == 3:
        if not route_number:
            violations['ROUTE_NUMBER must not be null when ROADWAY_TYPE=Route'].append(route_id)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE=Route'].append(route_id)
        if not signing and not re.match(r'^9\d{2}$', str(route_number)):
            violations[(
                'ROUTE_NUMBER must be a \'900\' route (i.e. 9xx) when ' +
                'ROADWAY_TYPE=Route and SIGNING is null'
            )].append(route_id)

    # Validations for ROADWAY_TYPE = Non-Mainline
    elif roadway_type == 5:
        if signing:
            violations['SIGNING must be null when ROADWAY_TYPE=Non-Mainline'].append(route_id)
        if route_number:
            violations['ROUTE_NUMBER must be null when ROADWAY_TYPE=Non-Mainline'].append(route_id)
        if route_suffix:
            violations['ROUTE_SUFFIX must be null when ROADWAY_TYPE=Non-Mainline'].append(route_id)
        if route_qualifier != 10:   # 10 is "No Qualifier"
            violations['ROUTE_QUALIFIER must be null when ROADWAY_TYPE=Non-Mainline'].append(route_id)
        if parkway_flag == 'T':      # T is "Yes"
            violations['PARKWAY_FLAG must be \'No\' when ROADWAY_TYPE=Non-Mainline'].append(route_id)
        if not roadway_feature:
            violations['ROADWAY_FEATURE must not be null when ROADWAY_TYPE=Non-Mainline'].append(route_id)

    else:
        raise AttributeError(
            'ROADWAY_TYPE is outside of the valid range. Must be one of (1, 2, 3, 4 ,5). ' +
            'Currently ROADWAY_TYPE={}'.format(roadway_type)
        )

    return violations
