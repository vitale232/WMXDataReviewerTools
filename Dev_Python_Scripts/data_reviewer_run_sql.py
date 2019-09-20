import datetime
import logging
import os
import time
import traceback

import arcpy



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

def co_dir_sql_result_to_feature_class(result_list, versioned_layer, reviewer_ws,
                                       session_name, origin_table, check_description,
                                       dot_id_index=0, county_order_index=1,
                                       log_name='', level='info',
                                       logger=None, arcpy_messages=None):
    dot_ids = [result_row[dot_id_index] for result_row in result_list]
    county_orders = [result_row[county_order_index] for result_row in result_list]

    where_clause = ''
    for dot_id, county_order in zip(dot_ids, county_orders):
        where_clause += '(DOT_ID = \'{}\' AND COUNTY_ORDER = \'{}\') OR '.format(dot_id, county_order)
    # Remove the extra ' AND ' from the where_clause from the last iteration
    where_clause = where_clause[:-4]
    log_it('{}: SQL Results where_clause: {}'.format(log_name, where_clause),
        level='info', logger=logger, arcpy_messages=arcpy_messages)

    arcpy.SelectLayerByAttribute_management(
        versioned_layer,
        'NEW_SELECTION',
        where_clause=where_clause
    )

    in_memory_fc = 'in_memory\\fc_{}'.format(int(time.time()))
    arcpy.CopyFeatures_management(
        versioned_layer,
        in_memory_fc
    )

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        session_name,
        in_memory_fc,
        'OBJECTID',
        origin_table,
        check_description
    )
    log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)
    return True

def rdwy_attrs_sql_result_to_feature_class(result_list, versioned_layer, reviewer_ws,
                                           session_name, origin_table, check_description,
                                           dot_id_index=0, log_name='', level='info',
                                           logger=None, arcpy_messages=None):
    dot_ids = [result_row[dot_id_index] for result_row in result_list]
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

    in_memory_fc = 'in_memory\\fc_{}'.format(int(time.time()))
    arcpy.CopyFeatures_management(
        versioned_layer,
        in_memory_fc
    )

    arcpy.WriteToReviewerTable_Reviewer(
        reviewer_ws,
        session_name,
        in_memory_fc,
        'OBJECTID',
        origin_table,
        check_description
    )
    log_it('', level='gp', logger=logger, arcpy_messages=arcpy_messages)

    return True

def initialize_logger(log_path=None, log_level=logging.INFO):
    if log_path and not os.path.isdir(os.path.dirname(os.path.abspath(log_path))):
        os.makedirs(os.path.dirname(os.path.abspath(log_path)))
    log_formatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )
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

def run_sql_validations(reviewer_ws, batch_job_file,
                        production_ws, job__id,
                        job__started_date, job__owned_by,
                        logger=None, messages=None):
    # These are the sql queries that need to be run against the full Milepoint table.
    #  Once the version name is determined, it will be plugged into the queries with the
    #  string format method
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
    
    arcpy.CheckOutExtension('datareviewer')

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
        'SQL validations: database {} | version: {}'.format(
            production_ws, production_ws_version
        )), level='info', logger=logger, arcpy_messages=messages)

    connection = arcpy.ArcSDESQLExecute(production_ws)
    # Change the SDE versioned view to the Workflow Manager version
    connection.execute("""EXEC ELRS.sde.set_current_version '{version_name}';""".format(
        version_name=production_ws_version
    ))

    unique_rdwy_attrs_result = connection.execute(unique_rdwy_attrs_sql)
    unique_co_dir_result = connection.execute(unique_co_dir_sql)

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
            'avitale',
            'Session 1',
            logger=logger,
            arcpy_messages=messages
        )

        if len(unique_rdwy_attrs_result) > 0:
            unique_rdwy_attrs_check_title = 'ROUTE_ID with improper roadway attrs across DOT_ID'

            rdwy_attrs_sql_result_to_feature_class(
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

            co_dir_sql_result_to_feature_class(
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
    arcpy.CheckInExtension('datareviewer')
    return True

def main(logger=None):
    if not logger:
        logger = logger.initialize_logger(logging.INFO)

    reviewer_ws = r'D:\Validation\dev\2019-09-20_Dev_2.gdb'
    batch_job_file = r'D:\Validation\dev\2019-09-16_GISSOR-Dev.rbj'
    production_ws = r'Database Connections\dev_elrs_ad_Lockroot.sde'
    job__id = 16843
    job__started_date = datetime.datetime(2019, 9, 17, 0, 0, 0)
    job__owned_by = r'SVC\AVITALE'

    run_sql_validations(
        reviewer_ws,
        batch_job_file,
        production_ws,
        job__id,
        job__started_date,
        job__owned_by,
        logger=logger
    )
    return


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    log_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        '..',
        'logs',
        'dr_dev_{year:04d}-{month:02d}-{day:02d}_{hour:02d}{min:02d}{sec:02d}.log'.format(
            year=start_time.year, month=start_time.month, day=start_time.day,
            hour=start_time.hour, min=start_time.minute, sec=start_time.second
        )
    ))
    logger = initialize_logger(log_path, logging.DEBUG)

    try:
        logger
    except NameError:
        logger = initialize_logger(logging.INFO)

    start_time = datetime.datetime.now()
    logger.info('Running script: {0} | Start time: {1}'.format(
        os.path.abspath(__file__), start_time)
    )

    main(logger=logger)

    end_time = datetime.datetime.now()
    logger.info('Completed at: {}. | Time to complete: {}'.format(
        end_time, end_time - start_time)
    )
