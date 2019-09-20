import datetime
import logging
import os
import time
import traceback

import arcpy


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
    unique_rdwy_attrs_fmt = (
        'EXEC ELRS.sde.set_current_version \'{version_name}\';\n' +
        'SELECT DOT_ID, COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ' +
            'ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))\n' +
        'FROM ELRS.elrs.LRSN_Milepoint_evw\n' +
        'WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND ' +
            '(TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)\n' +
        'GROUP BY DOT_ID\n' +
        'HAVING COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ' +
        'ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))>1;'
    )

    unique_co_dir_fmt = """
    EXEC ELRS.sde.set_current_version '{version_name}';

    SELECT DOT_ID, COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))
    FROM ELRS.elrs.LRSN_Milepoint_evw
    WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)
    GROUP BY DOT_ID
    HAVING COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))>1;"""

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

    milepoint_fcs = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
    if len(milepoint_fcs) == 1:
        milepoint_fc = milepoint_fcs[0]
    else:
        raise ValueError(
            'Too many feature classes were selected while trying to find LRSN_Milepoint. ' +
            'Selected FCs: {}'.format(milepoint_fcs)
        )

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

    unique_rdwy_attrs_sql = unique_rdwy_attrs_fmt.format(version_name=production_ws_version)
    log_it(unique_rdwy_attrs_sql, level='info', logger=logger, arcpy_messages=messages)

    log_it('Connecting to the production database/version: {}/{}'.format(
        production_ws, production_ws_version
    ), level='info', logger=logger, arcpy_messages=messages)

    connection = arcpy.ArcSDESQLExecute(production_ws)
    rdwy_attrs_response = connection.execute(unique_rdwy_attrs_sql)
    print(rdwy_attrs_response)

    return

def main(logger=None):
    if not logger:
        logger = logger.initialize_logger(logging.INFO)

    reviewer_ws = r'D:\Validation\dev\2019-09-17_Dev.gdb'
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
