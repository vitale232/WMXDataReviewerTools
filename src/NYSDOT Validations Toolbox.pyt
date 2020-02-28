import logging

import arcpy

import validation_helpers.utils as utils
import validation_helpers.validations as validations


class Toolbox(object):
    """
    This is the Python Toolbox definition that constructs the geoprocessing
    tools that can be used with WMX and ArcGIS Desktop.
    Visit the following link for information about how ArcGIS Python Toolboxes work:
    http://desktop.arcgis.com/en/arcmap/10.5/analyze/creating-tools/a-quick-tour-of-python-toolboxes.htm
    """
    def __init__(self):
        self.label = 'NYSDOT R&H Validation Tools'
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
            direction='Input',
        )

        job__owned_by_param = arcpy.Parameter(
            displayName='Editor Username (i.e. WMX Owned by User)',
            name='job__owned_by',
            datatype='GPString',
            parameterType='Required',
            direction='Input',
        )

        job__id_param = arcpy.Parameter(
            displayName='Workflow Manager Job ID',
            name='job__id',
            datatype='GPString',
            parameterType='Required',
            direction='Input',
        )

        production_ws_param = arcpy.Parameter(
            displayName='Production Workspace (SDE File)',
            name='production_ws',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input',
        )

        production_ws_version_param = arcpy.Parameter(
            displayName='Production WS Version (ELRS.Lockroot or Edit Version)',
            name='production_ws_version',
            datatype='GPString',
            parameterType='Required',
        )

        reviewer_ws_param = arcpy.Parameter(
            displayName='Reviewer Workspace (SDE File or DR Enabled FGDB)',
            name='reviewer_ws',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input',
        )

        log_path_param = arcpy.Parameter(
            displayName='Output Logfile Path (.txt)',
            name='log_path',
            datatype='DETextfile',
            parameterType='Optional',
            direction='Output',
            category='Logging'
        )

        log_level_param = arcpy.Parameter(
            displayName='Logging Level',
            name='log_level',
            datatype='GPString',
            parameterType='Required',
            category='Logging'
        )

        # Look in the ArcCatalog Database Connections folder for the appropriate SDE file (ELRS lockroot version)
        if arcpy.Exists(r'Database Connections\dev_elrs_ad_Lockroot.sde'):
            production_ws_param.value = r'Database Connections\dev_elrs_ad_Lockroot.sde'
        elif arcpy.Exists(r'Database Connections\dev_elrs_ad_lockroot.sde'):
            production_ws_param.value = r'Database Connections\dev_elrs_ad_lockroot.sde'
        elif arcpy.Exists(r'Database Connections\dev_elrs_ad_LockRoot.sde'):
            production_ws_param.value = r'Database Connections\dev_elrs_ad_LockRoot.sde'
        else:
            pass

        # Look in the ArcCatalog Database Connections folder for the appropriate SDE file (SDE DR database)
        if arcpy.Exists(r'Database Connections\dev_elrs_datareviewer_ad.sde'):
            reviewer_ws_param.value = r'Database Connections\dev_elrs_datareviewer_ad.sde'
        elif arcpy.Exists(r'Database Connections\dev_elrs_DataReviewer_ad.sde'):
            reviewer_ws_param.value = r'Database Connections\dev_elrs_DataReviewer_ad.sde'
        elif arcpy.Exists(r'Database Connections\dev_elrs_datareviewer_dr_user.sde'):
            reviewer_ws_param.value = r'Database Connections\dev_elrs_datareviewer_dr_user.sde'
        else:
            pass

        # Set options for production WS version
        production_ws_version_param.filter.type = 'ValueList'
        production_ws_version_param.filter.list = ['ELRS.Lockroot', 'Edit Version (Determined from WMX Job ID)']
        production_ws_version_param.value = 'ELRS.Lockroot'

        # Set options for log level param
        log_level_param.filter.type = 'ValueList'
        log_level_param.filter.list = ['DEBUG', 'INFO']
        log_level_param.value = 'INFO'

        params = [
            job__started_date_param, job__owned_by_param, job__id_param,
            production_ws_param, production_ws_version_param, reviewer_ws_param,
            log_path_param, log_level_param
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


class SupplementalParameters:
    """
    This is just a little convenience class to facilitate maintaining parameter code in one place.
    Some tools require only the parameters defined in the NYSDOTValidationsMixin, but others require all of the
    parameters from NYSDOTValidationMixin plus one or both of the parameters that are defined here.

    To take advantage of this class, in the Tool's getParameterInfo definition, you must call super().getParameterInfo
    on the NYSDOTValidationMixin. Then, you can instantiate this class, and use the required parameters' properties.
    The getParameterInfo must return a list of parameters, so take the return value from super() and the parameters
    that are stored on ``self`` as properties, and combine them into a final list of parameters.

    Example
    -------
    >>> class FooToolForBarBox(NYSDOTValidationsMixin, object):
    >>>     def getParameterInfo(self):
    >>>         params = super(ExecuteReviewerBatchJobOnEdits, self).getParameterInfo()
    >>>         
    >>>         supplemental_params = SupplementalParameters()
    >>>         full_db_flag_param = supplemental_params.full_db_flag_param
    >>>         
    >>>         return params + [ full_db_flag_param ]
    """
    full_db_flag_param = arcpy.Parameter(
            displayName='Run Validations on Full Geodatabase (Instead of edits)',
            name='full_db_flag',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input',
        )

    batch_job_file_param = arcpy.Parameter(
        displayName='Reviewer Batch Job Filepath (.rbj)',
        name='batch_job_file',
        datatype='DEFile',
        parameterType='Required',
        direction='Input',
    )


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
        production_ws_version_flag = parameters[4].valueAsText
        reviewer_ws = parameters[5].valueAsText
        log_path = parameters[6].valueAsText
        log_level = parameters[7].valueAsText

        if log_path == '':
            log_path = None

        if log_level.upper() == 'INFO':
            log_level = 20
        elif log_level.upper() == 'DEBUG':
            log_level = 10
        else:
            # Fall back to info level logging
            log_level = 20

        logger = utils.initialize_logger(log_path=log_path, log_level=log_level)

        # Determine which database version needs to be validated and save it as the production_ws_version variable
        if production_ws_version_flag == 'ELRS.Lockroot':
            production_ws_version = utils.get_lockroot_version(production_ws, production_ws_version_flag)
            # If the production_ws_version is not found, change the production_ws_version_flag to
            # force the following code block to execute (attempt to validate on edit version instead)
            if not production_ws_version:
                production_ws_version_flag = 'NotLockroot'

        if production_ws_version_flag != 'ELRS.Lockroot':
            user, production_ws_version = utils.get_user_and_version(
                job__owned_by,
                job__id,
                production_ws,
                logger=logger,
                arcpy_messages=messages
            )
        utils.log_it(
            'ExecuteNetworkSQLValidations.execute(): Generating versioned view of ' +
            'LRS Network | Database version: {}'.format(production_ws_version),
            level='info', logger=logger, arcpy_messages=messages)

        milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
            production_ws,
            production_ws_version,
        )
        utils.log_it(
            ('ExecuteNetworkSQLValidations.execute(): ' +
            'Found milepoint_fc and created versioned layer: {}'.format(milepoint_fc)),
            level='debug', logger=logger, arcpy_messages=messages)

        validations.run_sql_validations(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            production_ws_version=production_ws_version,
            version_milepoint_layer=version_milepoint_layer,
            milepoint_fc=milepoint_fc,
            logger=logger,
            messages=messages
        )

        arcpy.ClearWorkspaceCache_management()
        arcpy.Delete_management(version_milepoint_layer)

        utils.log_it('#'*4 + ' SQL validations completed without error! ' + '#'*4,
            level='info', logger=logger, arcpy_messages=messages)

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
        This tool has one additional parameter than the NYSDOTValidationsMixin, which
        is a flag to run the tool on the full database.

        The call to the `super` function returns a list of all of the parameters defined in
        the mixin. The additional parameter is defined here, and the two lists are combined
        and returned.
        """
        params = super(ExecuteRoadwayLevelAttributeValidations, self).getParameterInfo()

        supplemental_params = SupplementalParameters()
        full_db_flag_param = supplemental_params.full_db_flag_param

        return params + [ full_db_flag_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        production_ws_version_flag = parameters[4].valueAsText
        reviewer_ws = parameters[5].valueAsText
        log_path = parameters[6].valueAsText
        log_level = parameters[7].valueAsText
        full_db_flag = parameters[8].valueAsText

        # Make the arcpy parameter values Pythonic
        if full_db_flag == 'true':
            full_db_flag = True
        else:
            full_db_flag = False

        if log_path == '':
            log_path = None

        if log_level.upper() == 'INFO':
            log_level = 20
        elif log_level.upper() == 'DEBUG':
            log_level = 10
        else:
            # Fall back to info level logging
            log_level = 20

        logger = utils.initialize_logger(log_path=log_path, log_level=log_level)

        # Determine which database version needs to be validated and save it as the production_ws_version variable
        if production_ws_version_flag == 'ELRS.Lockroot':
            production_ws_version = utils.get_lockroot_version(production_ws, production_ws_version_flag)
            # If the production_ws_version is not found, change the production_ws_version_flag to
            # force the following code block to execute (attempt to validate on edit version instead)
            if not production_ws_version:
                production_ws_version_flag = 'NotLockroot'

        if production_ws_version_flag != 'ELRS.Lockroot':
            user, production_ws_version = utils.get_user_and_version(
                job__owned_by,
                job__id,
                production_ws,
                logger=logger,
                arcpy_messages=messages
            )
        utils.log_it(
            'ExecuteRoadwayLevelAttributeValidations.execute(): Generating versioned view of ' +
            'LRS Network | Database version: {}'.format(production_ws_version),
            level='info', logger=logger, arcpy_messages=messages)

        milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
            production_ws,
            production_ws_version,
        )

        utils.log_it(
            'ExecuteRoadwayLevelAttributeValidations.execute(): ' +
            'Found milepoint_fc and created versioned layer: {}'.format(milepoint_fc),
            level='debug', logger=logger, arcpy_messages=messages)

        validations.run_roadway_level_attribute_checks(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            production_ws_version=production_ws_version,
            version_milepoint_layer=version_milepoint_layer,
            milepoint_fc=milepoint_fc,
            full_db_flag=full_db_flag,
            logger=logger,
            messages=messages
        )

        arcpy.ClearWorkspaceCache_management()
        arcpy.Delete_management(version_milepoint_layer)

        utils.log_it('#'*4 + ' Roadway level validations completed without error! ' + '#'*4,
            level='info', logger=logger, arcpy_messages=messages)

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

        supplemental_params = SupplementalParameters()

        batch_job_file_param = supplemental_params.batch_job_file_param
        full_db_flag_param = supplemental_params.full_db_flag_param

        return params + [ batch_job_file_param, full_db_flag_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        production_ws_version_flag = parameters[4].valueAsText
        reviewer_ws = parameters[5].valueAsText
        log_path = parameters[6].valueAsText
        log_level = parameters[7].valueAsText
        batch_job_file = parameters[8].valueAsText
        full_db_flag = parameters[9].valueAsText

        if log_path == '':
            log_path = None

        if log_level.upper() == 'INFO':
            log_level = 20
        elif log_level.upper() == 'DEBUG':
            log_level = 10
        else:
            # Fall back to info level logging
            log_level = 20

        logger = utils.initialize_logger(log_path=log_path, log_level=log_level)

        # Determine which database version needs to be validated and save it as the production_ws_version variable
        if production_ws_version_flag == 'ELRS.Lockroot':
            production_ws_version = utils.get_lockroot_version(production_ws, production_ws_version_flag)
            # If the production_ws_version is not found, change the production_ws_version_flag to
            # force the following code block to execute (attempt to validate on edit version instead)
            if not production_ws_version:
                production_ws_version_flag = 'NotLockroot'

        if production_ws_version_flag != 'ELRS.Lockroot':
            user, production_ws_version = utils.get_user_and_version(
                job__owned_by,
                job__id,
                production_ws,
                logger=logger,
                arcpy_messages=messages
            )
        utils.log_it(
            'ExecuteReviewerBatchJobOnEdits.execute(): Generating versioned view of ' +
            'LRS Network | Database version: {}'.format(production_ws_version),
            level='info', logger=logger, arcpy_messages=messages)

        milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
            production_ws,
            production_ws_version,
        )
        utils.log_it(
            'ExecuteReviewerBatchJobOnEdits.execute(): ' +
            'Found milepoint_fc and created versioned layer: {}'.format(milepoint_fc),
            level='debug', logger=logger, arcpy_messages=messages)

        validations.run_batch_on_buffered_edits(
            reviewer_ws,
            batch_job_file,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            production_ws_version=production_ws_version,
            version_milepoint_layer=version_milepoint_layer,
            milepoint_fc=milepoint_fc,
            full_db_flag=full_db_flag,
            logger=logger,
            messages=messages
        )

        arcpy.ClearWorkspaceCache_management()
        arcpy.Delete_management(version_milepoint_layer)

        utils.log_it('#'*4 + ' Reviewer Batch Job completed without error! ' + '#'*4,
            level='info', logger=logger, arcpy_messages=messages)

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

        supplemental_params = SupplementalParameters()

        batch_job_file_param = supplemental_params.batch_job_file_param
        full_db_flag_param = supplemental_params.full_db_flag_param

        return params + [ batch_job_file_param, full_db_flag_param ]

    def execute(self, parameters, messages):
        job__started_date = parameters[0].valueAsText
        job__owned_by = parameters[1].valueAsText
        job__id = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        production_ws_version_flag = parameters[4].valueAsText
        reviewer_ws = parameters[5].valueAsText
        log_path = parameters[6].valueAsText
        log_level = parameters[7].valueAsText
        batch_job_file = parameters[8].valueAsText
        full_db_flag = parameters[9].valueAsText

        logger = utils.initialize_logger(log_path=log_path, log_level=log_level)

        # Convert to full_db_flag from an arcpy String type to a Python boolean
        if full_db_flag == 'true':
            full_db_flag = True
        else:
            full_db_flag = False

        if log_path == '':
            log_path = None

        # Translate readable log levels to the Python logging module coding
        if log_level.upper() == 'INFO':
            log_level = 20
        elif log_level.upper() == 'DEBUG':
            log_level = 10
        else:
            # Fall back to info level logging
            log_level = 20

        # Determine which database version needs to be validated and save it as the production_ws_version variable
        if production_ws_version_flag == 'ELRS.Lockroot':
            production_ws_version = utils.get_lockroot_version(production_ws, production_ws_version_flag)
            # If the production_ws_version is not found, change the production_ws_version_flag to
            # force the following code block to execute (attempt to validate on edit version instead)
            if not production_ws_version:
                production_ws_version_flag = 'NotLockroot'

        if production_ws_version_flag != 'ELRS.Lockroot':
            user, production_ws_version = utils.get_user_and_version(
                job__owned_by,
                job__id,
                production_ws,
                logger=logger,
                arcpy_messages=messages
            )

        utils.log_it(
            'ExecuteAllValidations.execute(): Generating versioned view of ' +
            'LRS Network | Database version: {}'.format(production_ws_version),
            level='info', logger=logger, arcpy_messages=messages)

        milepoint_fc, version_milepoint_layer = utils.get_version_milepoint_layer(
            production_ws,
            production_ws_version,
        )
        utils.log_it((
            'ExecuteAllValidations.execute(): Found milepoint_fc and created versioned layer: {}'.format(milepoint_fc)),
            level='debug', logger=logger, arcpy_messages=messages)

        validations.run_roadway_level_attribute_checks(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            production_ws_version=production_ws_version,
            version_milepoint_layer=version_milepoint_layer,
            milepoint_fc=milepoint_fc,
            full_db_flag=full_db_flag,
            logger=logger,
            messages=messages
        )

        validations.run_batch_on_buffered_edits(
            reviewer_ws,
            batch_job_file,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            production_ws_version=production_ws_version,
            version_milepoint_layer=version_milepoint_layer,
            milepoint_fc=milepoint_fc,
            full_db_flag=full_db_flag,
            logger=logger,
            messages=messages
        )

        validations.run_sql_validations(
            reviewer_ws,
            production_ws,
            job__id,
            job__started_date,
            job__owned_by,
            production_ws_version=production_ws_version,
            version_milepoint_layer=version_milepoint_layer,
            milepoint_fc=milepoint_fc,
            logger=logger,
            messages=messages
        )

        arcpy.Delete_management(version_milepoint_layer)
        arcpy.ClearWorkspaceCache_management()

        utils.log_it('#'*4 + ' All validations have run successfully! ' + '#'*4,
            level='info', logger=logger, arcpy_messages=messages)

        return True
