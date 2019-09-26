from collections import defaultdict
import datetime
import logging
import os
import re
import time
import traceback

import arcpy


class Toolbox(object):
    def __init__(self):
        self.label = 'NYSDOT Validations Toolbox'
        self.alias = 'validate'

        self.tools = [
            ExecuteReviewerBatchJobOnEdits, ExecuteNetworkSQLValidations,
            ExecuteRoadwayLevelAttributeValidations, ExecuteAllValidations,
        ]


class NYSDOTValidationsMixin(object):
    """
    Since the Data Reviewer jobs all require the same inputs, we'll use a Mixin
    to define the getParameterInfo and the isLicensed methods. Now it only needs to be
    maintained in one location
    """

    def getParameterInfo(self):
        job__started_date_param = arcpy.Parameter(
            displayName='Edits Start Date (e.g. WMX Job Creation Date)',
            name='job__started_date',
            datatype='GPDate',
            parameterType='Required',
            direction='Input'
        )

        job__owned_by_param = arcpy.Parameter(
            displayName='Editor Username (e.g. WMX Owned by User)',
            name='job__owned_by',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        job__id_param = arcpy.Parameter(
            displayName='Workflow Manager Job ID',
            name='job__id',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        production_ws_param = arcpy.Parameter(
            displayName='Production Workspace (SDE File)',
            name='production_ws',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input'
        )

        reviewer_ws_param = arcpy.Parameter(
            displayName='Reviewer Workspace',
            name='reviewer_ws',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input'
        )

        log_path_param = arcpy.Parameter(
            displayName='Output Logfile Path (.txt)',
            name='log_path',
            datatype='DETextfile',
            parameterType='Optional',
            direction='Output',
            category='Logging'
        )

        params = [
            job__started_date_param, job__owned_by_param, job__id_param,
            production_ws_param, reviewer_ws_param, log_path_param
        ]

        return params

    def isLicensed(self):
        try:
            if arcpy.CheckExtension('datareviewer') != 'Available':
                raise Exception
        except Exception:
            return False
        return True


class ExecuteNetworkSQLValidations(NYSDOTValidationsMixin, object):
    def __init__(self):
        self.label = 'Execute SQL Validations Against Network'
        self.description = (
            'Run SQL validations against the Milepoint network in the ELRS. '  +
            'These validations ensure that the roadway level attributes are correct, ' +
            'and the results are saved to the Data Reviewer session.'
        )
        self.canRunInBackground = False

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText

        if log_path == '':
            log_path = None

        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        run_sql_validations(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            logger=logger,
            messages=messages
        )

        return True


class ExecuteRoadwayLevelAttributeValidations(NYSDOTValidationsMixin, object):
    def __init__(self):
        self.label = 'Execute Roadway Level Attribute Validations'
        self.description = (
            'Run Python validations against routes that are newly created and ' +
            'edited since the creation of the current version. The validations ' +
            'ensure that the attributes on Milepoint are correct for a given roadway type'
        )
        self.canRunInBackground = False

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText

        if log_path == '':
            log_path = None

        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        run_roadway_level_attribute_checks(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            logger=logger,
            messages=messages
        )

        return True


class ExecuteReviewerBatchJobOnEdits(NYSDOTValidationsMixin, object):
    def __init__(self):
        self.label = 'Execute Reviewer Batch Job on R&H Edits'
        self.description = (
            'Determine which edits were made by the job\'s creator since the creation date, ' +
            'buffer the edits by 10 meters, and execute a Data Reviewer Batch Job with the ' +
            'buffer polygons input as the Data Reviewer Analysis Area.'
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        params = super(ExecuteReviewerBatchJobOnEdits, self).getParameterInfo()

        batch_job_file_param = arcpy.Parameter(
            displayName='Reviewer Batch Job Filepath (.rbj)',
            name='batch_job_file',
            datatype='DEFile',
            parameterType='Required',
            direction='Input'
        )

        return params + [ batch_job_file_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText
        batch_job_file = parameters[6].valueAsText

        if log_path == '':
            log_path = None

        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        run_batch_on_buffered_edits(
            reviewer_ws,
            batch_job_file,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            logger=logger,
            messages=messages
        )

        return True


class ExecuteAllValidations(NYSDOTValidationsMixin, object):
    def __init__(self):
        self.label = 'Execute All Validations'
        self.description = (
            'Run the Data Reviewer batch job, Milepoint SQL validations and ' +
            'Milepoint Roadway Level Attribute validations and save the results ' +
            'to the Data Reviewer session.'
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        params = super(ExecuteAllValidations, self).getParameterInfo()

        batch_job_file_param = arcpy.Parameter(
            displayName='Reviewer Batch Job Filepath (.rbj)',
            name='batch_job_file',
            datatype='DEFile',
            parameterType='Required',
            direction='Input'
        )

        return params + [ batch_job_file_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText
        batch_job_file = parameters[6].valueAsText

        if log_path == '':
            log_path = None

        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        run_batch_on_buffered_edits(
            reviewer_ws,
            batch_job_file,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            logger=logger,
            messages=messages
        )

        run_sql_validations(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            logger=logger,
            messages=messages
        )

        run_roadway_level_attribute_checks(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            logger=logger,
            messages=messages
        )

        return True

def run_batch_on_buffered_edits(reviewer_ws, batch_job_file,
                                production_ws, job__id,
                                job__started_date, job__owned_by,
                                logger=None, messages=None):
    try:
        if not logger:
            logger = initialize_logger(log_path=None, log_level=logging.INFO)

        log_it(('Calling run_batch_on_buffered_edits(): Selecting edits by this user ' +
                'in HDS_GENERAL_EDITING_JOB_{} and buffering by 10 meters'.format(job__id)),
                level='info', logger=logger, arcpy_messages=messages)

        arcpy.CheckOutExtension('datareviewer')

        # Set the database connection as the workspace. All table and FC references come from here
        arcpy.env.workspace = production_ws

        # Assemble the version name from its component parts. This is much easier than passing in
        #  the version name, as version names contain " and \ characters :-|
        if '\\' in job__owned_by:
            user = job__owned_by.split('\\')[1]
        else:
            user = job__owned_by
        production_ws_version = '"SVC\\{user}".HDS_GENERAL_EDITING_JOB_{job_id}'.format(
            user=user,
            job_id=job__id
        )
        log_it(('Reassembled [JOB:OWNED_BY] and [JOB:ID] WMX tokens to create production_ws_version: ' +
                '{}'.format(production_ws_version)),
                level='debug', logger=logger, arcpy_messages=messages)

        # Check that the production workspace contains the production workspace version
        version_names = arcpy.ListVersions(production_ws)
        if not production_ws_version in version_names:
            raise AttributeError(
                'The version name \'{}\' does not exist in the workspace \'{}\'.'.format(
                    production_ws_version,
                    production_ws
                ) + ' Available versions include: {}'.format(version_names)
            )

        # Select the milepoint LRS feature class from the workspace
        milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
        if len(milepoint_fcs) == 1:
            milepoint_fc = milepoint_fcs[0]
        else:
            raise ValueError(
                'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
                'Selected FCs: {}'.format(milepoint_fcs)
            )

        # Filter out edits made by this user on this version since its creation
        where_clause = 'EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\''.format(
            date=job__started_date,
            user=user
        )
        log_it('Using where_clause on {}: {}'.format(milepoint_fc, where_clause),
            level='debug', logger=logger, arcpy_messages=messages)

        # Explicitly change version to the input version, as the SDE file could point to anything
        sde_milepoint_layer = arcpy.MakeFeatureLayer_management(
            milepoint_fc,
            'milepoint_layer_{}'.format(int(time.time()))
        )
        version_milepoint_layer = arcpy.ChangeVersion_management(
            sde_milepoint_layer,
            'TRANSACTIONAL',
            version_name=production_ws_version
        )
        version_select_milepoint_layer = arcpy.SelectLayerByAttribute_management(
            version_milepoint_layer,
            'NEW_SELECTION',
            where_clause=where_clause
        )
        log_it('{count} features selected'.format(
            count=arcpy.GetCount_management(version_select_milepoint_layer).getOutput(0)),
            level='debug', logger=logger, arcpy_messages=messages)

        # Create buffer polygons of the edited features. These will be used as the DR analysis_area
        log_it('Buffering edited routes by 10 meters',
            level='info', logger=logger, arcpy_messages=messages)
        buffer_polygons = 'in_memory\\mpbuff_{}'.format(int(time.time()))

        # Set the output coordinate reference for the Buffer_analysis call
        arcpy.env.outputCoordinateSystem = arcpy.Describe(milepoint_fc).spatialReference

        arcpy.Buffer_analysis(
            version_select_milepoint_layer,
            buffer_polygons,
            '10 Meters'
        )
        log_it('', level='gp', logger=logger, arcpy_messages=messages)

        # Data Reviewer WMX tokens are only supported in the default DR Step Types. We must back out
        #  the session name from the DR tables
        reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
            user=user,
            job_id=job__id
        )
        reviewer_fields = ['SESSIONID', 'USERNAME', 'SESSIONNAME']
        session_table = os.path.join(reviewer_ws, 'GDB_REVSESSIONTABLE')
        with arcpy.da.SearchCursor(session_table, reviewer_fields, where_clause=reviewer_where_clause) as curs:
            for row in curs:
                session_id = row[0]

        if session_id:
            reviewer_session = 'Session {session_id} : {job_id}'.format(
                session_id=session_id,
                job_id=job__id
            )
            log_it('Reviewer session name determined to be \'{}\''.format(reviewer_session),
                    level='debug', logger=logger, arcpy_messages=messages)
        else:
            raise ValueError('Could not determine the session ID with where_clause: {}'.format(reviewer_where_clause))

        reviewer_results = arcpy.ExecuteReviewerBatchJob_Reviewer(
            reviewer_ws,
            reviewer_session,
            batch_job_file,
            production_workspace=production_ws,
            analysis_area=buffer_polygons,
            changed_features='ALL_FEATURES',
            production_workspaceversion=production_ws_version
        )
        log_it('', level='gp', logger=logger, arcpy_messages=messages)

        try:
            # Try to cleanup the runtime environment
            arcpy.CheckInExtension('datareviewer')
            arcpy.env.workspace = r'in_memory'
            fcs = arcpy.ListFeatureClasses()
            log_it('in_memory fcs: {}'.format(fcs), level='debug', logger=logger, arcpy_messages=messages)
            for in_memory_fc in fcs:
                try:
                    arcpy.Delete_management(in_memory_fc)
                    log_it(' deleted: {}'.format(in_memory_fc), level='debug', logger=logger, arcpy_messages=messages)
                except:
                    pass
        except:
            pass

        log_it('GP Tool success at: {}'.format(datetime.datetime.now()),
            level='info', logger=logger, arcpy_messages=messages)
        return True

    except Exception as exc:
        log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
        raise exc

def run_sql_validations(reviewer_ws, production_ws, job__id,
                        job__started_date, job__owned_by,
                        logger=None, messages=None):
    try:
        # These are the sql queries that need to be run against the full Milepoint table.
        arcpy.CheckOutExtension('datareviewer')

        unique_rdwy_attrs_sql= (
            'SELECT DOT_ID, COUNTY_ORDER, COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG)) ' +
            'FROM ELRS.elrs.LRSN_Milepoint_evw ' +
            'WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP) ' +
            'GROUP BY DOT_ID, COUNTY_ORDER ' +
            'HAVING COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ' +
            'ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))>1;'
        )

        unique_co_dir_sql = (
            'SELECT DOT_ID, COUNTY_ORDER, DIRECTION, COUNT (1) ' +
            'FROM ELRS.elrs.LRSN_Milepoint_evw ' +
            'WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP) ' +
            'GROUP BY DOT_ID, COUNTY_ORDER, DIRECTION ' +
            'HAVING COUNT (1)>1;'
        )

        log_it(('Calling run_sql_validations(): Running SQL queries against the ' +
            'ELRS in HDS_GENERAL_EDITING_JOB_{} and writing '.format(job__id)) +
            'results to Data Reviewer session',
                level='info', logger=logger, arcpy_messages=messages)

        arcpy.env.workspace = production_ws

        if '\\' in job__owned_by:
            user = job__owned_by.split('\\')[1]
        else:
            user = job__owned_by
        production_ws_version = '"SVC\\{user}".HDS_GENERAL_EDITING_JOB_{job_id}'.format(
            user=user,
            job_id=job__id
        )
        log_it(('Reassembled [JOB:OWNED_BY] and [JOB:ID] WMX tokens to create production_ws_version: ' +
                '{}'.format(production_ws_version)),
                level='debug', logger=logger, arcpy_messages=messages)

        version_names = arcpy.ListVersions(production_ws)
        if not production_ws_version in version_names:
            raise AttributeError(
                'The version name \'{}\' does not exist in the workspace \'{}\'.'.format(
                    production_ws_version,
                    production_ws
                ) + ' Available versions include: {}'.format(version_names)
            )

        log_it(('Connecting to the production database and executing ' +
            'SQL validations -> database: {} | version: {}'.format(
                production_ws, production_ws_version
            )), level='info', logger=logger, arcpy_messages=messages)

        connection = arcpy.ArcSDESQLExecute(production_ws)
        # Change the SDE versioned view to the Workflow Manager version
        connection.execute("""EXEC ELRS.sde.set_current_version '{version_name}';""".format(
            version_name=production_ws_version
        ))

        unique_rdwy_attrs_result = connection.execute(unique_rdwy_attrs_sql)
        unique_co_dir_result = connection.execute(unique_co_dir_sql)

        if isinstance(unique_co_dir_result, bool):
            unique_co_dir_result = []
        if isinstance(unique_rdwy_attrs_result, bool):
            unique_rdwy_attrs_result = []

        if len(unique_rdwy_attrs_result) > 0 or len(unique_co_dir_result) > 0:
            # Create a versioned arcpy feature layer of the Milepoint feature class
            milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
            if len(milepoint_fcs) == 1:
                milepoint_fc = milepoint_fcs[0]
            else:
                raise ValueError(
                    'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
                    'Selected FCs: {}'.format(milepoint_fcs)
                )

            sde_milepoint_layer = arcpy.MakeFeatureLayer_management(
                milepoint_fc,
                'milepoint_layer_{}'.format(int(time.time()))
            )
            version_milepoint_layer = arcpy.ChangeVersion_management(
                sde_milepoint_layer,
                'TRANSACTIONAL',
                version_name=production_ws_version
            )

            reviewer_session_name = get_reviewer_session_name(
                reviewer_ws,
                user,
                job__id,
                logger=logger,
                arcpy_messages=messages
            )

            if len(unique_rdwy_attrs_result) > 0:
                unique_rdwy_attrs_check_title = 'ROUTE_ID with improper roadway attrs across DOT_ID'

                rdwy_attrs_sql_result_to_reviewer_table(
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

                co_dir_sql_result_to_reviewer_table(
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
            arcpy.env.workspace = r'in_memory'
            fcs = arcpy.ListFeatureClasses()
            log_it('in_memory fcs: {}'.format(fcs), level='debug', logger=logger, arcpy_messages=messages)
            for in_memory_fc in fcs:
                try:
                    arcpy.Delete_management(in_memory_fc)
                    log_it(' deleted: {}'.format(in_memory_fc), level='debug', logger=logger, arcpy_messages=messages)
                except:
                    pass
        except:
            pass

    except Exception as exc:
        log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
        raise exc

    return True

def run_roadway_level_attribute_checks(reviewer_ws, production_ws, job__id,
                                       job__started_date, job__owned_by,
                                       logger=None, messages=None):
    if not logger:
        logger = initialize_logger(log_path=None, log_level=logging.INFO)

    log_it(
        'Calling run_roadway_level_attribute_checks(): Selecting newly created ' +
        'or edited routes by this user in this version and validating their attributes',
        level='info', logger=logger, arcpy_messages=messages)

    arcpy.CheckOutExtension('datareviewer')

    # Set the database connection as the workspace. All table and FC references come from here
    arcpy.env.workspace = production_ws

    # Assemble the version name from its component parts. This is much easier than passing in
    #  the version name, as version names contain " and \ characters :-|
    if '\\' in job__owned_by:
        user = job__owned_by.split('\\')[1]
    else:
        user = job__owned_by
    production_ws_version = '"SVC\\{user}".HDS_GENERAL_EDITING_JOB_{job_id}'.format(
        user=user,
        job_id=job__id
    )
    log_it(('Reassembled [JOB:OWNED_BY] and [JOB:ID] WMX tokens to create production_ws_version: ' +
            '{}'.format(production_ws_version)),
            level='debug', logger=logger, arcpy_messages=messages)

    version_names = arcpy.ListVersions(production_ws)
    if not production_ws_version in version_names:
        raise AttributeError(
            'The version name \'{}\' does not exist in the workspace \'{}\'.'.format(
                production_ws_version,
                production_ws
            ) + ' Available versions include: {}'.format(version_names)
        )

    milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
    if len(milepoint_fcs) == 1:
        milepoint_fc = milepoint_fcs[0]
    else:
        raise ValueError(
            'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
            'Selected FCs: {}'.format(milepoint_fcs)
        )

    where_clause = '(EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\')'.format(
        date=job__started_date,
        user=user
    )
    log_it('Using where_clause on {}: {}'.format(milepoint_fc, where_clause),
        level='debug', logger=logger, arcpy_messages=messages)

    # Explicitly change version to the input version, as the SDE file could point to anything
    sde_milepoint_layer = arcpy.MakeFeatureLayer_management(
        milepoint_fc,
        'milepoint_layer_{}'.format(int(time.time()))
    )
    version_milepoint_layer = arcpy.ChangeVersion_management(
        sde_milepoint_layer,
        'TRANSACTIONAL',
        version_name=production_ws_version
    )
    version_select_milepoint_layer = arcpy.SelectLayerByAttribute_management(
        version_milepoint_layer,
        'NEW_SELECTION',
        where_clause=where_clause
    )
    log_it('{count} features selected'.format(
        count=arcpy.GetCount_management(version_select_milepoint_layer).getOutput(0)),
        level='debug', logger=logger, arcpy_messages=messages)

    attribute_fields = [
        'ROADWAY_TYPE', 'ROUTE_ID', 'DOT_ID', 'COUNTY_ORDER', 'SIGNING', 'ROUTE_NUMBER',
        'ROUTE_SUFFIX', 'ROUTE_QUALIFIER', 'PARKWAY_FLAG', 'ROADWAY_FEATURE',
    ]
    violations = defaultdict(list)
    with arcpy.da.SearchCursor(version_select_milepoint_layer, attribute_fields) as curs:
        for row in curs:
            roadway_type = row[0]
            attributes = row[1:]
            results = validate_by_roadway_type(roadway_type, attributes)
            for rule_rids in results.items():
                violations[rule_rids[0]].append(*rule_rids[1])

    session_name = get_reviewer_session_name(
        reviewer_ws,
        user,
        job__id,
        logger=logger,
        arcpy_messages=messages
    )

    roadway_level_attribute_result_to_reviewer_table(
        violations,
        version_milepoint_layer,
        reviewer_ws,
        session_name,
        milepoint_fc,
        level='info',
        logger=logger,
        arcpy_messages=messages
    )

    return True

def roadway_level_attribute_result_to_reviewer_table(result_dict, versioned_layer, reviewer_ws,
                                                    session_name, origin_table,
                                                    level='info', logger=None, arcpy_messages=None):
    for rule_rids in result_dict.items():
        check_description = rule_rids[0]
        route_ids = rule_rids[1]
        if not route_ids:
            continue
        where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "')"

        log_it('{}: roadway_level_attribute_result where_clause={}'.format(rule_rids[0], where_clause),
            level='info', logger=logger, arcpy_messages=arcpy_messages)

        arcpy.SelectLayerByAttribute_management(
            versioned_layer,
            'NEW_SELECTION',
            where_clause=where_clause
        )

        in_memory_fc = to_in_memory_fc(versioned_layer)

        arcpy.WriteToReviewerTable_Reviewer(
            reviewer_ws,
            session_name,
            in_memory_fc,
            'ORIG_OBJECTID',
            origin_table,
            check_description
        )
        log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)
        try:
            arcpy.Delete_management(in_memory_fc)
        except:
            pass
    return True

def validate_by_roadway_type(roadway_type, attributes):
    """
    The attribute fields must be in the following order:
    signing, route_number, route_suffix, route_qualifier, parkway_flag, and roadway_feature
    Otherwise these validations are invalid!!!!!
    """
    rid, dot_id, county_order, signing, route_number, route_suffix, route_qualifier, parkway_flag, roadway_feature = attributes
    if roadway_type not in [1, 2, 3, 4, 5]:
        raise AttributeError(
            'ROADWAY_TYPE is outside of the valid range. Must be one of (1, 2, 3, 4 ,5). ' +
            'Currently ROADWAY_TYPE={}'.format(roadway_type)
        )

    violations = defaultdict(list)

    if not re.match(r'^\d{9}$', str(rid)):
        violations['ROUTE_ID must be a nine digit number'].append(rid)
    if not re.match(r'^\d{6}$', str(dot_id)):
        violations['DOT_ID must be a six digit number'].append(rid)
    if not re.match(r'^\d{2}$', str(county_order)):
        violations['COUNTY_ORDER must be a zero padded two digit number (e.g. \'01\')'].append(rid)

    if roadway_type == 1 or roadway_type == 2:     # Road or Ramp
        if signing:
            violations['SIGNING must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(rid)
        if route_number:
            violations['ROUTE_NUMBER must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(rid)
        if route_suffix:
            violations['ROUTE_SUFFIX must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(rid)
        if route_qualifier != 10:    # 10 is "No Qualifier"
            violations[(
                'ROUTE_QUALIFIER must be \'No Qualifier\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')'
            )].append(rid)
        if parkway_flag == 'T':      # T is "Yes"
            violations['PARKWAY_FLAG must be \'No\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(rid)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')'].append(rid)

    if roadway_type == 3:     # Route
        if not route_number:
            violations['ROUTE_NUMBER must not be null when ROADWAY_TYPE=Route'].append(rid)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE=Route'].append(rid)
        if not signing and not re.match(r'^9\d{2}$', str(route_number)):
            violations[(
                'ROUTE_NUMBER must be a \'900\' route (i.e. 9xx) when ' +
                'ROADWAY_TYPE=Route and SIGNING is null'
            )].append(rid)

    if roadway_type == 5:    # Non-Mainline
        if signing:
            violations['SIGNING must be null when ROADWAY_TYPE=Non-Mainline'].append(rid)
        if route_number:
            violations['ROUTE_NUMBER must be null when ROADWAY_TYPE=Non-Mainline'].append(rid)
        if route_suffix:
            violations['ROUTE_SUFFIX must be null when ROADWAY_TYPE=Non-Mainline'].append(rid)
        if route_qualifier != 10:   # 10 is "No Qualifier"
            violations['ROUTE_QUALIFIER must be null when ROADWAY_TYPE=Non-Mainline'].append(rid)
        if parkway_flag == 'T':      # T is "Yes"
            violations['PARKWAY_FLAG must be \'No\' when ROADWAY_TYPE=Non-Mainline'].append(rid)
        if not roadway_feature:
            violations['ROADWAY_FEATURE must not be null when ROADWAY_TYPE=Non-Mainline'].append(rid)

    return violations

def get_reviewer_session_name(reviewer_ws, user, job_id, logger=None, arcpy_messages=None):
    reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
        user=user,
        job_id=job_id
    )
    reviewer_fields = ['SESSIONID', 'USERNAME', 'SESSIONNAME']
    session_table = os.path.join(reviewer_ws, 'GDB_REVSESSIONTABLE')
    with arcpy.da.SearchCursor(session_table, reviewer_fields, where_clause=reviewer_where_clause) as curs:
        for row in curs:
            session_id = row[0]
    if session_id:
        reviewer_session = 'Session {session_id} : {job_id}'.format(
            session_id=session_id,
            job_id=job_id
        )
        log_it('Reviewer session name determined to be \'{}\''.format(reviewer_session),
                level='debug', logger=logger, arcpy_messages=arcpy_messages)
    else:
        raise ValueError('Could not determine the session ID with where_clause: {}'.format(reviewer_where_clause))
    return reviewer_session

def to_in_memory_fc(layer, new_field='ORIG_OBJECTID', check_fields=['ROUTE_ID', 'OBJECTID']):
    in_memory_fc = 'in_memory\\fc_{}'.format(int(time.time()))

    arcpy.CopyFeatures_management(
        layer,
        in_memory_fc
    )

    field_names = [field.name for field in arcpy.ListFields(layer)]

    if not new_field in field_names:
        arcpy.AddField_management(
            in_memory_fc,
            new_field,
            'LONG'
        )
        oids = {row[0]: row[1] for row in arcpy.da.SearchCursor(layer, check_fields)}
        update_fields = [new_field, check_fields[0]]
        with arcpy.da.UpdateCursor(in_memory_fc, update_fields) as curs:
            for row in curs:
                route_id = row[1]
                new_field_value = oids[route_id]
                curs.updateRow([new_field_value, route_id])

    return in_memory_fc

def co_dir_sql_result_to_reviewer_table(result_list, versioned_layer, reviewer_ws,
                                        session_name, origin_table, check_description,
                                        dot_id_index=0, county_order_index=1,
                                        log_name='', level='info',
                                        logger=None, arcpy_messages=None):
    dot_ids = [result_row[dot_id_index] for result_row in result_list]
    county_orders = [result_row[county_order_index] for result_row in result_list]

    where_clause = ''
    for dot_id, county_order in zip(dot_ids, county_orders):
        where_clause += '(DOT_ID = \'{}\' AND COUNTY_ORDER = \'{}\') OR '.format(dot_id, county_order)
    # Remove the extra ' OR ' from the where_clause from the last iteration
    where_clause = where_clause[:-4]
    log_it('{}: SQL Results where_clause: {}'.format(log_name, where_clause),
        level='info', logger=logger, arcpy_messages=arcpy_messages)

    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=where_clause
    )

    in_memory_fc = to_in_memory_fc(versioned_layer)

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        session_name,
        in_memory_fc,
        'ORIG_OBJECTID',
        origin_table,
        check_description
    )
    log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)
    return True

def rdwy_attrs_sql_result_to_reviewer_table(result_list, versioned_layer, reviewer_ws,
                                            session_name, origin_table, check_description,
                                            dot_id_index=0, log_name='', level='info',
                                            logger=None, arcpy_messages=None):
    dot_ids = [result_row[dot_id_index] for result_row in result_list]
    # Account for the rare case where a DOT_ID does not exist, which will be caught in another validation
    dot_ids = ['' for dot_id in dot_ids if dot_id is None]

    where_clause = "DOT_ID IN ('" + "', '".join(dot_ids) + "')"
    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=where_clause
    )
    fields = [
        'ROUTE_ID', 'SIGNING', 'ROUTE_NUMBER', 'ROUTE_SUFFIX',
        'ROADWAY_TYPE', 'ROUTE_QUALIFIER', 'ROADWAY_FEATURE', 'PARKWAY_FLAG'
    ]
    with arcpy.da.SearchCursor(versioned_layer, fields) as curs:
        milepoint_attrs = {row[0]: list(row[1:]) for row in curs}

    unique_attrs = [list(x) for x in set(tuple(x) for x in milepoint_attrs.values())]
    milepoint_attr_values = milepoint_attrs.values()
    unique_attrs_occurrence_count = [
        [milepoint_attr_values.count(row), row] for row in milepoint_attr_values
    ]
    route_ids = [
        milepoint_attrs.keys()[milepoint_attrs.values().index(row[1])] for row in unique_attrs_occurrence_count
        if row[0] == 1
    ]

    output_where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "')"

    log_it('{}: SQL Results where_clause: {}'.format(log_name, output_where_clause),
        level='info', logger=logger, arcpy_messages=arcpy_messages)

    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=output_where_clause
    )

    in_memory_fc = to_in_memory_fc(versioned_layer)

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        session_name,
        in_memory_fc,
        'ORIG_OBJECTID',
        origin_table,
        check_description
    )
    log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)

    return True

def initialize_logger(log_path=None, log_level=logging.INFO):
    if log_path and not os.path.isdir(os.path.dirname(os.path.abspath(log_path))):
        os.makedirs(os.path.dirname(os.path.abspath(log_path)))
    log_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    root_logger = logging.getLogger()

    if log_path:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    root_logger.setLevel(level=log_level)

    return root_logger

def log_it(message, level='info', logger=None, arcpy_messages=None):
    if level.lower() == 'info':
        if logger:
            logging.info(message)
        if arcpy_messages:
            arcpy_messages.addMessage(message)
    elif level.lower() == 'debug':
        if logger:
            logging.debug(message)
        if arcpy_messages:
            arcpy_messages.addMessage(message)
    elif level.lower() == 'error':
        if logger:
            logging.error(message)
        if arcpy_messages:
            arcpy_messages.addErrorMessage(message)
    elif level.lower() == 'gp':
        if arcpy_messages:
            arcpy_messages.addGPMessages()
    else:
        raise ValueError('Parameter \'level\' must be one of (info, debug, error, gp)')

    return True

