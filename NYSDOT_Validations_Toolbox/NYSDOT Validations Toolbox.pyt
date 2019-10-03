from collections import defaultdict
import datetime
import logging
import os
import re
import time
import traceback

import arcpy


class Toolbox(object):
    """
    This is the Python Toolbox definition that constructs the geoprocessing 
    tools that can be used with WMX and ArcGIS Desktop.
    Visit the following link for information about how ArcGIS Python Toolboxes work:
    http://desktop.arcgis.com/en/arcmap/10.5/analyze/creating-tools/a-quick-tour-of-python-toolboxes.htm
    """
    def __init__(self):
        self.label = 'NYSDOT Validations Toolbox'
        self.alias = 'validate'

        self.tools = [
            ExecuteReviewerBatchJobOnEdits, ExecuteNetworkSQLValidations,
            ExecuteRoadwayLevelAttributeValidations, ExecuteAllValidations,
        ]


class NYSDOTValidationsMixin(object):
    """
    These validation tools require many of the same inputs and licenses. I've implemented
    a mixin class to make them easier to maintain. Pass the Mixin class into the tools' class 
    definitions as the first positional argument, and they will inherit these definitions. By
    defining these parameters in one place, maintenance is easier.
    """

    def getParameterInfo(self):
        job__started_date_param = arcpy.Parameter(
            displayName='Edits Start Date (i.e. WMX Job Creation Date)',
            name='job__started_date',
            datatype='GPDate',
            parameterType='Required',
            direction='Input'
        )

        job__owned_by_param = arcpy.Parameter(
            displayName='Editor Username (i.e. WMX Owned by User)',
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
        """
        All of these tools require a Data Reviewer license.
        """
        try:
            if arcpy.CheckExtension('datareviewer') != 'Available':
                raise Exception
        except Exception:
            return False
        return True


class ExecuteNetworkSQLValidations(NYSDOTValidationsMixin, object):
    """
    This tool executes two separate SQL queries against the ELRS database. The 
    queries are intended to identify invalid attributes at the DOT_ID level. 
    One query for example, one DOT_ID should have a consistent value for SIGNING, ROUTE_NUMBER,
    ROUTE_SUFFIX, ROADWAY_FEATURE, PARKWAY_FLAG. The second query checks for unique DIRECTION
    codes at the DOT_ID/COUNTY_ORDER level. That is to say, two routes with the same 
    DOT_ID and COUNTY_ORDER should not have the same DIRECTION code.
    """
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
    """
    This tool validates the roadway level attributes of the Milepoint network.
    Roadway level attributes refers to the fields on the LRSN_Milepoint table.
    The validations are implemented in Python for ease of maintenance. These validations
    will not change for the lifetime of the current ELRS implementation, thus it seemed 
    easier to implement them in Python rather than using a Reviewer Batch Job.

    The validations take place at the ROADWAY_TYPE level. Each ROADWAY_TYPE has specific
    expectations for fields like SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, etc. If the user
    has incorrectly entered the ROADWAY_TYPE, these validations are nonsense.
    """
    def __init__(self):
        self.label = 'Execute Roadway Level Attribute Validations'
        self.description = (
            'Run Python validations against routes that are newly created and ' +
            'edited since the creation of the current version. The validations ' +
            'ensure that the attributes on Milepoint are correct for a given roadway type'
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        """
        This tool has one additional parameter than the NYSDTOValidationsMixin, which
        is a flag to run the tool on the full database.

        The call to the `super` function returns a list of all of the parameters defined in 
        the mixin. The additional parameter is defined here, and the two lists are combined 
        and returned.
        """
        params = super(ExecuteRoadwayLevelAttributeValidations, self).getParameterInfo()

        full_db_flag_param = arcpy.Parameter(
            displayName='Run Validations on Full Geodatabase (Instead of edits)',
            name='full_db_flag',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input'
        )

        return params + [ full_db_flag_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText
        full_db_flag = parameters[6].valueAsText

        if full_db_flag == 'true':
            full_db_flag = True
        else:
            full_db_flag = False

        if log_path == '':
            log_path = None

        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        run_roadway_level_attribute_checks(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            full_db_flag=full_db_flag,
            logger=logger,
            messages=messages
        )

        return True


class ExecuteReviewerBatchJobOnEdits(NYSDOTValidationsMixin, object):
    """
    This tool manages the execution of a Data Reviewer Batch Job. 
    Using ArcGIS Desktop, you can create a .rbj file that specifies specific 
    pre-built Esri routines for geospatial and attribute validation (called checks)
    and their input  parameters.  Any valid RBJ can be passed into this tool as a parameter.
    If the full_db_flag is set, the RBJ will execute against the entire geodatabase (this
    process is quite slow, taking about 7.5 hours). If the full_db_flag is False,
    the tool will use the input parameters to determine which features have been edited
    since the creating of the WMX job, buffer those features, and run the RBJ on
    all features that intersect the buffer polygons.
    """
    def __init__(self):
        self.label = 'Execute Reviewer Batch Job on R&H Edits'
        self.description = (
            'Determine which edits were made by the job\'s creator since the creation date, ' +
            'buffer the edits by 10 meters, and execute a Data Reviewer Batch Job with the ' +
            'buffer polygons input as the Data Reviewer Analysis Area.'
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        """
        This tool requires two additional parameters than are present in the mixin class.
        Capture the mixin paramters using `super`, define the additional parameters, 
        and combine/return the full list of parameters.
        """
        params = super(ExecuteReviewerBatchJobOnEdits, self).getParameterInfo()

        batch_job_file_param = arcpy.Parameter(
            displayName='Reviewer Batch Job Filepath (.rbj)',
            name='batch_job_file',
            datatype='DEFile',
            parameterType='Required',
            direction='Input'
        )

        full_db_flag_param = arcpy.Parameter(
            displayName='Run Validations on Full Geodatabase',
            name='full_db_flag',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input'
        )

        return params + [ batch_job_file_param, full_db_flag_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText
        batch_job_file = parameters[6].valueAsText
        full_db_flag = parameters[7].valueAsText

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
            full_db_flag=full_db_flag,
            logger=logger,
            messages=messages
        )

        return True


class ExecuteAllValidations(NYSDOTValidationsMixin, object):
    """
    This is the tool that will be used in the Workflow Manager workflows. It executes all of
    NYSDOT's validation tools against the ELRS geodatabase.

    This tool is essentially a wrapper function. It requires all of the parameters that
    could possible be defined in ExecuteReviewerBatchJobOnEdits, ExecuteNetworkSQLValidations,
    or ExecuteRoadwayLevelAttributeValidations. The parameters are then passed into the functions
    that make up the base functionality of those tools and executed accordingly.
    """
    def __init__(self):
        self.label = 'Execute All Validations'
        self.description = (
            'Run the Data Reviewer batch job, Milepoint SQL validations and ' +
            'Milepoint Roadway Level Attribute validations and save the results ' +
            'to the Data Reviewer session.'
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        """
        Get the mixin classes parameters using `super`, then add the other required parameters
        to the list and return it.
        """
        params = super(ExecuteAllValidations, self).getParameterInfo()

        batch_job_file_param = arcpy.Parameter(
            displayName='Reviewer Batch Job Filepath (.rbj)',
            name='batch_job_file',
            datatype='DEFile',
            parameterType='Required',
            direction='Input'
        )

        full_db_flag_param = arcpy.Parameter(
            displayName='Run Validations on Full Geodatabase',
            name='full_db_flag',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input'
        )

        return params + [ batch_job_file_param, full_db_flag_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        reviewer_ws = parameters[4].valueAsText
        log_path = parameters[5].valueAsText
        batch_job_file = parameters[6].valueAsText
        full_db_flag = parameters[7].valueAsText

        if full_db_flag == 'true':
            full_db_flag = True
        else:
            full_db_flag = False

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
            full_db_flag=full_db_flag,
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
            full_db_flag,
            logger=logger,
            messages=messages
        )

        return True


class VersionDoesNotExistError(Exception):
    """
    This exception is raised when the specified database version does not
    exist in the ELRS SDE database.
    """
    pass


class NoReviewerSessionIDError(Exception):
    """
    This exception is raised when the Reviewer Session ID cannot be backed out of the
    Data Reviewer workspace tables.
    """
    pass


def run_batch_on_buffered_edits(reviewer_ws, batch_job_file,
                                production_ws, job__id,
                                job__started_date, job__owned_by,
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
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: When successful, the function returns True
    """
    try:
        if not logger:
            logger = initialize_logger(log_path=None, log_level=logging.INFO)

        arcpy.CheckOutExtension('datareviewer')

        # Set the database connection as the workspace. All table and FC references come from here
        arcpy.env.workspace = production_ws

        user, production_ws_version = get_user_and_version(
            job__owned_by,
            job__id,
            production_ws,
            logger=None,
            arcpy_messages=None
        )

        if not full_db_flag:
            log_it(('Calling run_batch_on_buffered_edits(): Selecting edits by this {} '.format(user) +
                    'in {} and buffering by 10 meters'.format(production_ws_version)),
                    level='info', logger=logger, arcpy_messages=messages)
        else:
            log_it(('Calling run_batch_on_buffered_edits(): Running Reviewer Batch Job on ' +
                'all features in {}'.format(production_ws_version)),
                level='info', logger=logger, arcpy_messages=messages)

        # Select the milepoint LRS feature class from the workspace
        milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
        if len(milepoint_fcs) == 1:
            milepoint_fc = milepoint_fcs[0]
        else:
            raise ValueError(
                'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
                'Selected FCs: {}'.format(milepoint_fcs)
            )

        if not full_db_flag:
            # Filter out edits made by this user on this version since its creation
            where_clause = 'EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\' AND TO_DATE IS NULL'.format(
                date=job__started_date,
                user=user.upper()
            )
        else:
            where_clause = None

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
        if where_clause:
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
                log_it(('0 features were identified as edited since {}. '.format(job__started_date) +
                    'Exiting without running validations!'),
                    level='warn', logger=logger, arcpy_messages=messages)
                return False

            log_it('{count} features selected'.format(count=feature_count),
                level='debug', logger=logger, arcpy_messages=messages)

            log_it('Buffering edited routes by 10 meters',
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
            log_it('', level='gp', logger=logger, arcpy_messages=messages)
        else:
            area_of_interest = arcpy.Describe(version_milepoint_layer).extent

        # Data Reviewer WMX tokens are only supported in the default DR Step Types. We must back out
        #  the session name from the DR tables
        reviewer_session = get_reviewer_session_name(
            reviewer_ws,
            user,
            job__id,
            logger=logger,
            arcpy_messages=messages
        )

        log_it(('Calling ExecuteReviewerBatchJob_Reviewer ' +
            'geoprocessing tool at {}'.format(datetime.datetime.now())),
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
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
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

        user, production_ws_version = get_user_and_version(
            job__owned_by,
            job__id,
            production_ws,
            logger=None,
            arcpy_messages=None
        )

        log_it(('Connecting to the production database and executing ' +
            'SQL validations -> database: {} | version: {}'.format(
                production_ws, production_ws_version
            )), level='info', logger=logger, arcpy_messages=messages)

        connection = arcpy.ArcSDESQLExecute(production_ws)
        # Change the SDE versioned view to the Workflow Manager version
        connection.execute('EXEC ELRS.sde.set_current_version \'{version_name}\';'.format(
            version_name=production_ws_version
        ))

        unique_rdwy_attrs_result = connection.execute(unique_rdwy_attrs_sql)
        unique_co_dir_result = connection.execute(unique_co_dir_sql)

        # If the query succeeds but the response is empty, arcpy returns a python
        #  boolean type with a True value. Convert booleans to an empty list to 
        #  simplify the results handling logic.
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
                                       job__started_date, job__owned_by, full_db_flag=False,
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
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.
    
    Returns
    -------
    :returns bool: When successful, the function returns True
    """
    try:
        if not logger:
            logger = initialize_logger(log_path=None, log_level=logging.INFO)

        log_it(
            'Calling run_roadway_level_attribute_checks(): Selecting newly created ' +
            'or edited routes by this user in this version and validating their attributes',
            level='info', logger=logger, arcpy_messages=messages)

        arcpy.CheckOutExtension('datareviewer')

        # Set the database connection as the workspace. All table and FC references come from here
        arcpy.env.workspace = production_ws

        user, production_ws_version = get_user_and_version(
            job__owned_by,
            job__id,
            production_ws,
            logger=logger,
            arcpy_messages=messages
        )

        milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
        if len(milepoint_fcs) == 1:
            milepoint_fc = milepoint_fcs[0]
        else:
            raise ValueError(
                'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
                'Selected FCs: {}'.format(milepoint_fcs)
            )

        # If the full_db_flag is True, run the validations on all active routes (routes with no TO_DATE).
        #  Otherwise, follow the typical pattern of selecting the data edited by this user
        #  in this version since it was created.
        if full_db_flag:
            where_clause = 'TO_DATE IS NULL'
        else:
            where_clause = 'EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\' AND TO_DATE IS NULL'.format(
                date=job__started_date,
                user=user.upper()
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

        # If these fields in precisely this order are not passed to the SearchCursor,
        #  the validate_roadway_type function will fail. Any updates to either function
        #  should be careful with the field orders.
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
            base_where_clause=where_clause,
            level='info',
            logger=logger,
            arcpy_messages=messages
        )
    except Exception as exc:
        log_it(traceback.format_exc(), level='error', logger=logger, arcpy_messages=messages)
        raise exc

    return True

def check_for_version(production_ws_version, production_ws, version_names):
    """
    Check if the production_ws_version exists in the production_ws. If not, raise
    a custom exception

    Arguments
    ---------
    :param production_ws_version: The version name that's expected to exist in the production_ws
    :param production_ws: Filepath to the SDE file pointing to the correct database. The version of the SDE filepath
        does not matter as long as it exists. The function will change to the user's version.
    :param version_names: A list or tuple of the available versions in the database

    Returns
    -------
    :returns bool: When successful, the function returns True
    :raises VersionDoesNotExistError: Raises exception if version does not exist in the production_ws
    """
    if not production_ws_version in version_names:
        raise VersionDoesNotExistError(
            'The version name \'{}\' does not exist in the workspace \'{}\'.'.format(
                production_ws_version,
                production_ws
            ) + ' Available versions include: {}'.format(version_names)
        )
    return True

def get_user_and_version(job__owned_by, job__id, production_ws, logger=None, arcpy_messages=None):
    """
    This function uses the WMX tokens and the production workspace to determine the "short username",
    which is how ArcGIS handles user names, as well as the version name. The short username
    is a derivative of the full username that is used in the version names that are created by
    WMX. An example full username is "SVC\AVITALE", whereas the short username is just AVITALE.
    
    Since the version names contain so many special characters, passing in the [JOB:VERSION_NAME] token
    via a WMX Geoprocessing Tool was creating errors. WMX handles the arguments as string, joined by a space
    character. Rather than hack at that for days, I decided to just take the component information
    and plug it into a pre-formatted string to get the version name.

    There is a small bug in WMX. Depending on how the job is assigned, the username will be all
    caps (expected) or all lowercase (not expected). Due to that, this function tries to use
    the short username as passed in to create the version name. If that doesn't work, it tries to use 
    the short username in all caps. If that doesn't work, it tries to use all lowercase. If that 
    doesn't work, it throws its hands up in the air and complains with an Exception.

    Arguments
    ---------
    :param job__owned_by: The username from the WMX [JOB:OWNED_BY] token.
    :param job__id: The [JOB:ID] WMX token or number if executed manually.
    :param production_ws: Filepath to the SDE file pointing to the correct database. The version of the SDE filepath
        does not matter as long as it exists. The function will change to the user's version.

    Returns
    -------
    :returns tuple: Returns a tuple containing the short username and the name of the
        version, which is confirmed to exist in the production_ws. The return order
        will always be user, production_ws_version
    :raises VersionDoesNotExistError: Raises this exception when the version name
        that's been generated in the code does not exist in the production_ws
    """
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

    version_names = arcpy.ListVersions(production_ws)
    try:
        check_for_version(production_ws_version, production_ws, version_names)
    except VersionDoesNotExistError:
        user = user.upper()
        production_ws_version = '"SVC\\{user}".HDS_GENERAL_EDITING_JOB_{job_id}'.format(
            user=user,
            job_id=job__id
        )
        try:
            check_for_version(production_ws_version, production_ws, version_names)
        except VersionDoesNotExistError:
            user = user.lower()
            production_ws_version = '"SVC\\{user}".HDS_GENERAL_EDITING_JOB_{job_id}'.format(
                user=user,
                job_id=job__id
            )

    check_for_version(production_ws_version, production_ws, version_names)

    log_it(('Reassembled [JOB:OWNED_BY] and [JOB:ID] WMX tokens to create production_ws_version: ' +
            '{}'.format(production_ws_version)),
            level='debug', logger=logger, arcpy_messages=arcpy_messages)

    return user, production_ws_version
        
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
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
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
            where_clause = 'SIGNING IS NULL AND ROADWAY_TYPE IN (1, 2) AND TO_DATE IS NULL'

        elif check_description == 'ROUTE_SUFFIX must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'ROUTE_SUFFIX IS NOT NULL AND ROADWAY_TYPE IN (1, 2) AND TO_DATE IS NULL'
        
        elif check_description == 'ROADWAY_FEATURE must be null when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'ROADWAY_FEATURE IS NOT NULL AND ROADWAY_TYPE IN (1, 2) AND TO_DATE IS NULL'
        
        elif check_description == 'ROUTE_QUALIFIER must be \'No Qualifier\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = '(ROUTE_QUALIFIER <> 10 OR ROUTE_QUALIFIER IS NULL) AND ROADWAY_TYPE IN (1, 2) AND TO_DATE IS NULL'
        
        elif check_description == 'PARKWAY_FLAG must be \'No\' when ROADWAY_TYPE in (\'Road\', \'Ramp\')':
            where_clause = 'PARKWAY_FLAG = \'T\' AND ROADWAY_TYPE IN (1, 2) AND TO_DATE IS NULL'

        else:
            where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "') AND TO_DATE IS NULL"

        if base_where_clause:
            violations_where_clause = '({base_where}) AND ({validation_where})'.format(
                base_where=base_where_clause,
                validation_where=where_clause
            )
        else:
            violations_where_clause = where_clause


        log_it('{}: roadway_level_attribute_result where_clause={}'.format(check_description, violations_where_clause),
            level='info', logger=logger, arcpy_messages=arcpy_messages)

        arcpy.SelectLayerByAttribute_management(
            versioned_layer,
            'NEW_SELECTION',
            where_clause=violations_where_clause
        )

        in_memory_fc = to_in_memory_fc(versioned_layer)

        arcpy.WriteToReviewerTable_Reviewer(
            reviewer_ws,
            reviewer_session,
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
            rid, dot_id, county_order, signing, route_number, route_suffix, route_qualifier, parkway_flag, roadway_feature
        ]
    
    Returns
    -------
    :returns defaultdict(list): This function returns a default dict that contains a list of the offending ROUTE_IDs
        as the dict items, and the rule(s) that was validated as the default dict keys.
    :raises AttributeError: Raises an AttributeError if the roadway_type is not within the valid range
    """
    rid, dot_id, county_order, signing, route_number, route_suffix, route_qualifier, parkway_flag, roadway_feature = attributes
    if roadway_type not in [1, 2, 3, 4, 5]:
        raise AttributeError(
            'ROADWAY_TYPE is outside of the valid range. Must be one of (1, 2, 3, 4 ,5). ' +
            'Currently ROADWAY_TYPE={}'.format(roadway_type)
        )

    violations = defaultdict(list)

    # All validations regardless of ROADWAY_TYPE
    if not re.match(r'^\d{9}$', str(rid)):
        violations['ROUTE_ID must be a nine digit number'].append(rid)
    if not re.match(r'^\d{6}$', str(dot_id)):
        violations['DOT_ID must be a six digit number'].append(rid)
    if not re.match(r'^\d{2}$', str(county_order)):
        violations['COUNTY_ORDER must be a zero padded two digit number (e.g. \'01\')'].append(rid)
    
    if county_order and int(county_order) == 0:
        violations['COUNTY_ORDER must be greater than \'00\''].append(rid)
    if county_order and int(county_order) > 28:
        violations['COUNTY_ORDER should be less than \'29\''].append(rid)

    # Validations for ROADWAY_TYPE = Road or Ramp
    if roadway_type == 1 or roadway_type == 2:
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

    # Validations for ROADWAY_TYPE = Route
    elif roadway_type == 3:
        if not route_number:
            violations['ROUTE_NUMBER must not be null when ROADWAY_TYPE=Route'].append(rid)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE=Route'].append(rid)
        if not signing and not re.match(r'^9\d{2}$', str(route_number)):
            violations[(
                'ROUTE_NUMBER must be a \'900\' route (i.e. 9xx) when ' +
                'ROADWAY_TYPE=Route and SIGNING is null'
            )].append(rid)

    # Validations for ROADWAY_TYPE = Non-Mainline
    elif roadway_type == 5:
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

    else:
        raise AttributeError(
            'ROADWAY_TYPE is outside of the valid range. Must be one of (1, 2, 3, 4 ,5). ' +
            'Currently ROADWAY_TYPE={}'.format(roadway_type)
        )

    return violations


def query_reviewer_table(reviewer_ws, reviewer_where_clause, messages=None):
    """
    Passing the [REVSESSION:ID] token from WMX was not producing the desired result, so 
    I've resorted to reading the session ID directly from the Reviewer Workspace. The session ID
    is saved in a reviewer workspace table called GDB_REVSESSIONTABLE. The relevant fields include the
    SESSIONID (what we're after), the USERNAME, and the SESSIONNAME. The reviewer_where_clause is typically
    of the construct: USERNAME=short_username AND SESSIONNAME=job__id

    Arguments
    ---------
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param reviewer_where_clause: The ArcGIS where clause used to determine the correct Reviewer Session ID

    Keyword Arguments
    -----------------
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns int or str: Returns an integer or string with representing the desired Reviewer Session ID
    :raises NoReviewerSessionIDError: If the session_id cannot be determined, a NoReviewerSessionIDError
        Exception is raised
    """
    reviewer_fields = ['SESSIONID', 'USERNAME', 'SESSIONNAME']
    session_table = os.path.join(reviewer_ws, 'GDB_REVSESSIONTABLE')

    with arcpy.da.SearchCursor(session_table, reviewer_fields, where_clause=reviewer_where_clause) as curs:
        for row in curs:
            session_id = row[0]
    try:
        session_id
    except UnboundLocalError:
        raise NoReviewerSessionIDError('Could not determine the session ID with where_clause: {}'.format(reviewer_where_clause))
    return session_id

def get_reviewer_session_name(reviewer_ws, user, job_id, logger=None, arcpy_messages=None):
    """
    This function manages the retrieval of the full Reviewer Session Name from the Data Reviewer
    Workspace. Due to a small WMX bug, the short username can be CamelCase, ALL CAPS, or lowercase.
    To workaround the bug, this function first tries the short username as it's passed in. If that 
    doesn't work, it tries the short username in all lowercase. If that doesn't work, it tries the
    short username in all caps. If that doesn't work, it quits with an error.

    Arguments
    ---------
    :param reviewer_ws: Filepath to a Data Reviewer enabled geodatabase. Currently use file geodatabases, will
        eventually use an SDE filepath
    :param user: The short username, that's typically generated by splitting the [JOB:OWNED_BY] WMX token
        on the backslash character and taking the element with index 1
    
    Keyword Arguments
    -----------------
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
    :param arcpy_messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.
    
    Returns
    -------
    :returns str: Returns a string representing the full Data Reviewer Session name, which can be
        passed to the Data Reviewer Geoprocessing tools
    :raises NoReviewerSessionIDError: If the session_id cannot be determined while constructing the full session name,
        a NoReviewerSessionIDError Exception is raised
    """
    try:
        reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
            user=user,
            job_id=job_id
        )
        session_id = query_reviewer_table(reviewer_ws, reviewer_where_clause, messages=arcpy_messages)
    except NoReviewerSessionIDError:
        try:
            reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
                user=user.lower(),
                job_id=job_id
            )

            session_id = query_reviewer_table(reviewer_ws, reviewer_where_clause, messages=arcpy_messages)
        except NoReviewerSessionIDError:
            reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
                user=user.upper(),
                job_id=job_id
            )

            session_id = query_reviewer_table(reviewer_ws, reviewer_where_clause, messages=arcpy_messages)

    try:
        reviewer_session = 'Session {session_id} : {job_id}'.format(
            session_id=session_id,
            job_id=job_id
        )
        log_it('Reviewer session name determined to be \'{}\''.format(reviewer_session),
                level='debug', logger=logger, arcpy_messages=arcpy_messages)
    except:
        raise NoReviewerSessionIDError('Could not determine the session ID with where_clause: {}'.format(reviewer_where_clause))
    return reviewer_session

def to_in_memory_fc(layer, new_field='ORIG_OBJECTID', check_fields=['ROUTE_ID', 'OBJECTID']):
    """
    This function creates an in memory feature class of the features selected in the `layer`
    parameter. Data Reviewer writes the OBJECTID of the identified features to the reviewer workspace,
    so the function also adds a new column to the in memory feature class called ORIG_OBJECTID by default.
    The new field is populated with the object ID of the input layer, so that the in_memory_fc.ORIG_OBJECTID
    field will identify the correct features in Milepoint when committed to the reviewer table.

    Arguments
    ---------
    :param layer: An arcpy Feature Layer that has the features you would like to write to the 
        in memory feature class selected
    
    Keyword Arguments
    -----------------
    :param new_field: Defaults to ORIG_OBJECTID. The name of the new field that will be added to
        the returned in memory feature class. It will be populated with the input `layer`'s OBJECTID
    :param check_fields: Defaults to a list of ['ROUTE_ID', 'OBJECTID']. These fields are passed into the
        SearchCursor that reads the `layer`. The first field in the list will be used as the key value
        in a dictionary that tracks the OBJECTIDs, the second field is the feature you would like
        to use to populate the new column identified by the `new_field` param
    
    Returns
    -------
    :returns in_memory_fc: A string pointing to the newly created in memory feature class
    """
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
    :param level: Defaults to 'info'. The string identifying the log level, passed to the `log_it` function
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
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
        where_clause += '(DOT_ID = \'{}\' AND COUNTY_ORDER = \'{}\' AND TO_DATE IS NULL) OR '.format(dot_id, county_order)
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
        reviewer_session,
        in_memory_fc,
        'ORIG_OBJECTID',
        origin_table,
        check_description
    )
    log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)
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
    :param level: Defaults to 'info'. The string identifying the log level, passed to the `log_it` function
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
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
    where_clause = "DOT_ID IN ('" + "', '".join(dot_ids) + "') AND TO_DATE IS NULL"
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

    output_where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "') AND TO_DATE IS NULL"

    log_it('{}: SQL Results where_clause: {}'.format(log_name, output_where_clause),
        level='info', logger=logger, arcpy_messages=arcpy_messages)

    if len(route_ids) == 0:
        log_it('    0 violations were found. Exiting with success code.',
            level='info', logger=logger, arcpy_messages=arcpy_messages)
        return True

    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=output_where_clause
    )

    in_memory_fc = to_in_memory_fc(versioned_layer)

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        reviewer_session,
        in_memory_fc,
        'ORIG_OBJECTID',
        origin_table,
        check_description
    )
    log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)

    return True

def initialize_logger(log_path=None, log_level=logging.INFO):
    """
    A function to initialize a logger from the Python logging module. If no
    parameters are passed in, the logger will default to console logging. Optionally, filepath
    logging can be turned on by passing a log_path. The default logging level is info,
    but this can be changed by passing in a different logging.level value, or their
    corresponding int values

    Keyword Arguments
    ---------
    :param log_path: Defaults to None, which means no file logging. Otherwise, a valid filepath
        on disk where you would like the logger to store the log file
    :param log_level: Defaults to logging.INFO. Can be changed to any valid log level supported
        by the base Python logging module
    
    Returns
    -------
    :returns logger: Returns a Python logging object.
    """
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
    """
    This function will log a message to the Python logger object and/or the
    arcpy Python Toolbox messages (the GP Tool's dialog) depending on the
    input parameters.

    Arguments
    ---------
    :param message: A string that will be committed to the activated loggers

    Keyword Arguments
    -----------------
    :param level: A string equal to 'info', 'debug', 'error', 'warn', or 'gp' (case insensitive)
        which will determine the level of logging for both arcpy and the Python logger
    :param logger: Defaults to None. If set, should be a filepath pointing to a location on disk to output
        the log file.
    :param messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns bool: Returns True when successful
    """
    if not level in ('info', 'debug', 'error', 'warn', 'gp'):
        raise ValueError('Parameter \'level\' must be one of (info, debug, error, warn, gp)')

    if logger:
        logger_level = logger.level
    else:
        # Python logging module represents INFO logging as 20 and DEBUG as 10.
        #  If there is no Python logger, fall back to info level logging
        logger_level = 20

    arcpy_message = '{datetime} [{level:<5}]  {message}'.format(
        datetime=datetime.datetime.now(),
        level=level.upper(),
        message=message
    )

    if level.lower() == 'info' and logger_level <= 20:
        if logger:
            logging.info(message)
        if arcpy_messages:
            arcpy_messages.addMessage(arcpy_message)
    elif level.lower() == 'debug' and logger_level <= 10:
        if logger:
            logging.debug(message)
        if arcpy_messages:
            arcpy_messages.addMessage(arcpy_message)
    elif level.lower() == 'error':
        if logger:
            logging.error(message)
        if arcpy_messages:
            arcpy_messages.addErrorMessage(arcpy_message)
    elif level.lower() == 'warn':
        if logger:
            logger.warn(message)
        if arcpy_messages:
            arcpy_messages.addWarningMessage(arcpy_message)
    elif level.lower() == 'gp':
        if arcpy_messages:
            arcpy_messages.addMessage(arcpy_message)
            arcpy_messages.addGPMessages()
            message = arcpy.GetMessages()
            logger.info(message)
    else:
        pass

    return True
