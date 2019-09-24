from collections import defaultdict
import datetime
import logging
import os
import re
import sys
import time

import arcpy

sys.path.append(r'D:\Python')
import common


def run_new_route_validations(reviewer_ws, production_ws, job__id,
                              job__started_date, job__owned_by,
                              logger=None, messages=None):
    if not logger:
        logger = common.initialize_logger(log_path=None, log_level=logging.INFO)

    logger.info(
        'Calling run_new_route_validations(): Selecting newly created routes by this user ' +
        'in this version and validating their attributes'
    )

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
        'ROADWAY_TYPE', 'ROUTE_ID', 'SIGNING', 'ROUTE_NUMBER', 'ROUTE_SUFFIX',
        'ROUTE_QUALIFIER', 'PARKWAY_FLAG', 'ROADWAY_FEATURE',
    ]
    violations = defaultdict(list)
    with arcpy.da.SearchCursor(version_select_milepoint_layer, attribute_fields) as curs:
        for row in curs:
            roadway_type = row[0]
            attributes = row[1:]
            results = validate_by_roadway_type(roadway_type, attributes)
            for rule_rids in results.items():
                violations[rule_rids[0]].append(rule_rids[1])
    
    session_name = get_reviewer_session_name(
        reviewer_ws,
        user,
        job__id,
        logger=logger,
        arcpy_messages=messages
    )

    for rule_rids in violations.items():
        roadway_level_attribute_result_to_feature_class(
            violations,
            version_milepoint_layer,
            reviewer_ws,
            session_name,
            milepoint_fc,
            level='info',
            logger=logger,
            arcpy_messages=messages
        )

def roadway_level_attribute_result_to_feature_class(result_dict, versioned_layer, reviewer_ws,
                                                    session_name, origin_table,
                                                    level='info', logger=None, arcpy_messages=None):
    for rule_rids in result_dict.items():
        check_description = rule_rids[0]
        route_ids = rule_rids[1]
        where_clause = "ROUTE_ID IN ('" + "', '".join(route_ids) + "')"

        log_it('roadway level attrs where_clause={}'.format(where_clause),
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
        try:
            arcpy.Delete_management(in_memory_fc)
        except:
            pass
    return True

def validate_by_roadway_type(roadway_type, attributes):
    # The attribute fields must be in the following order:
    # signing, route_number, route_suffix, route_qualifier, parkway_flag, and roadway_feature
    # Otherwise these validations are invalid!!!!!
    rid, signing, route_number, route_suffix, route_qualifier, parkway_flag, roadway_feature = attributes
    if roadway_type not in [1, 2, 3, 4, 5]:
        raise AttributeError(
            'ROADWAY_TYPE is outside of the valid range. Must be one of (1, 2, 3, 4 ,5). ' +
            'Currently ROADWAY_TYPE={}'.format(roadway_type)
        )

    violations = defaultdict(list)
    if roadway_type == 1 or roadway_type == 2:     # Road or Ramp
        if signing:
            violations['SIGNING must be null when ROADWAY_TYPE=Road'].append(rid)
        if route_number:
            violations['ROUTE_NUMBER must be null when ROADWAY_TYPE=Road'].append(rid)
        if route_suffix:
            violations['ROUTE_SUFFIX must be null when ROADWAY_TYPE=Road'].append(rid)
        if route_qualifier != 10:    # 10 is "No Qualifier"
            violations['ROUTE_QUALIFIER must be \'No Qualifier\' when ROADWAY_TYPE=Road'].append(rid)
        if parkway_flag:
            violations['PARKWAY_FLAG must be null when ROADWAY_TYPE=Road'].append(rid)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE=Road'].append(rid)
    if roadway_type == 3:     # Route
        if not route_number:
            violations['ROUTE_NUMBER must not be null when ROADWAY_TYPE=Route'].append(rid)
        if roadway_feature:
            violations['ROADWAY_FEATURE must be null when ROADWAY_TYPE=Route'].append(rid)
        if not signing and not re.match(r'9\d{2}', str(route_number)):
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
        if parkway_flag:
            violations['PARKWAY_FLAG must be null when ROADWAY_TYPE=Non-Mainline'].append(rid)
        if not roadway_feature:
            violations['ROADWAY_FEATURE must not be null when ROADWAY_TYPE=None-Mainline'].append(rid)

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

def main(logger=None):
    if not logger:
        logger = common.initialize_logger(logging.INFO)
    
    # Required DR inputs
    # reviewer_ws = r'D:\Validation\dev\2019-03-27_Dev.gdb'
    reviewer_ws = r'D:\Validation\dev\2019-09-17_Dev.gdb'
    reviewer_session = 'Script Session {}'.format(int(time.time()))
    batch_job_file = r'D:\Validation\dev\2019-09-16_GISSOR-Dev.rbj'
    production_ws = r'Database Connections\dev_elrs_ad_Lockroot.sde'
    job__id = 18044
    # production_ws_version = r'"SVC\AVITALE".HDS_GENERAL_EDITING_JOB_13242'

    # WMX inputs. Variable names represent WMX tokens.
    #  e.g. [JOB:STARTED_DATE] => job__started_date
    job__started_date = datetime.datetime(2019, 9, 16, 0, 0, 0)
    job__owned_by = r'SVC\AVITALE'

    run_new_route_validations(
        reviewer_ws,
        production_ws,
        job__id,
        job__started_date,
        job__owned_by,
        logger=logger,
        messages=None
    )

    return True


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
    logger = common.initialize_logger(log_path, logging.DEBUG)

    try:
        logger
    except NameError:
        logger = common.initialize_logger(logging.INFO)

    start_time = datetime.datetime.now()
    logger.info('Running script: {0} | Start time: {1}'.format(
        os.path.abspath(__file__), start_time)
    )

    main(logger=logger)

    end_time = datetime.datetime.now()
    logger.info('Completed at: {}. | Time to complete: {}'.format(
        end_time, end_time - start_time)
    )
