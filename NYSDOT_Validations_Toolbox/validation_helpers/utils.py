import datetime
import logging
import os
import time

import arcpy

from validation_helpers.active_routes import ACTIVE_ROUTES_QUERY


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

def get_version_milepoint_layer(production_ws, production_ws_version,
                                wildcard='*LRSN_Milepoint',
                                logger=None, arcpy_messages=None):
    """
    Get the name of the LRSN from the Roads and Highways ALRS using the wildcard keyward argument, and return
    the feature class name. Generate a versioned FeatureLayer of the feature class, and return it.

    Arguments
    ---------
    :param production_ws: Filepath to the SDE file pointing to the correct database. The version of the SDE filepath
        does not matter as long as it exists. The function will change to the user's version.
    :param production_ws_version: The version name that's expected to exist in the production_ws

    Keyword Arguments
    -----------------
    :param wildcard: A wildcard expression that's passed to `arcpy.ListFeatureClasses` which identifies the
        network feature class from the R&H ALRS
    :param logger: Defaults to None. If set, should be Python logging module logger object.
    :param arcpy_messages: Defaults to None. If set, should refer to the arcpy.Messages variable that is present
        in the `execute` method of Python Toolboxes.

    Returns
    -------
    :returns tuple: Returns a tuple of the milepoint_fc variable (ALRS Network Feature class) and a versioned
        `arcpy.FeatureLayer` pointing to the milepoint_fc in the production_ws, referencing the production_ws_version
    """
    original_ws = arcpy.env.workspace
    arcpy.env.workspace = production_ws

    milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses(wildcard)]
    if len(milepoint_fcs) == 1:
        milepoint_fc = milepoint_fcs[0]
    else:
        raise ValueError(
            'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
            'Selected FCs: {}'.format(milepoint_fcs)
        )

    log_it('found milepoint FC: {}'.format(milepoint_fc),
        level='warn', logger=logger, arcpy_messages=arcpy_messages)
    sde_milepoint_layer = arcpy.MakeFeatureLayer_management(
        milepoint_fc,
        'milepoint_layer_{}'.format(int(time.time()))
    )

    version_milepoint_layer = arcpy.ChangeVersion_management(
        sde_milepoint_layer,
        'TRANSACTIONAL',
        version_name=production_ws_version
    )

    arcpy.env.workspace = original_ws

    return milepoint_fc, version_milepoint_layer

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

def query_reviewer_table(reviewer_ws, reviewer_where_clause, logger=None, messages=None):
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
    # Get the current workspace, set the new workspace to the Reviewer Workspace, then switch
    #  back to the original at the end of the function's work
    original_ws = arcpy.env.workspace
    arcpy.env.workspace = reviewer_ws
    session_tables = [table for table in arcpy.ListTables('*GDB_REVSESSIONTABLE')]

    # If reviewer_ws is an SDE workspace, session_tables will be a list with one element containing
    #  the databasename.databaseuser.GDB_REVSESSIONTABLE
    #  File geodatabases do not return this table when using ListTables, so if the length is not
    #  1, try to join the reviewer_ws directly to the table name
    if len(session_tables) == 1:
        session_table = session_tables[0]
    elif arcpy.Exists(os.path.join(reviewer_ws, 'GDB_REVSESSIONTABLE')):
        session_table = os.path.join(reviewer_ws, 'GDB_REVSESSIONTABLE')
    else:
        raise ValueError(
            'Too many or too few tables were selected while trying to find GDB_REVSESSIONTABLE. ' +
            'Selected tables: {}'.format(session_tables)
        )
    log_it('Reviewer Session table determined to be: {}'.format(session_table),
        level='debug', logger=logger, arcpy_messages=messages)
    reviewer_fields = ['SESSIONID', 'USERNAME', 'SESSIONNAME']
    with arcpy.da.SearchCursor(session_table, reviewer_fields, where_clause=reviewer_where_clause) as curs:
        for row in curs:
            session_id = row[0]
    try:
        session_id
    except UnboundLocalError:
        raise NoReviewerSessionIDError(
            'Could not determine the session ID with where_clause: {}'.format(reviewer_where_clause)
        )

    arcpy.env.workspace = original_ws
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
    :param logger: Defaults to None. If set, should be Python logging module logger object.
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

        session_id = query_reviewer_table(
            reviewer_ws,
            reviewer_where_clause,
            logger=logger,
            messages=arcpy_messages
        )
    except NoReviewerSessionIDError:
        try:
            reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
                user=user.lower(),
                job_id=job_id
            )

            session_id = query_reviewer_table(
                reviewer_ws,
                reviewer_where_clause,
                logger=logger,
                messages=arcpy_messages
            )
        except NoReviewerSessionIDError:
            reviewer_where_clause = 'USERNAME = \'{user}\' AND SESSIONNAME = \'{job_id}\''.format(
                user=user.upper(),
                job_id=job_id
            )

            session_id = query_reviewer_table(
                reviewer_ws,
                reviewer_where_clause,
                logger=logger,
                messages=arcpy_messages
            )
    try:
        reviewer_session = 'Session {session_id} : {job_id}'.format(
            session_id=session_id,
            job_id=job_id
        )
        log_it('Reviewer session name determined to be \'{}\''.format(reviewer_session),
                level='debug', logger=logger, arcpy_messages=arcpy_messages)
    except:
        raise NoReviewerSessionIDError(
            'Could not determine the session ID with where_clause: {}'.format(reviewer_where_clause)
        )
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
    :param logger: Defaults to None. If set, should be Python logging module logger object.
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
