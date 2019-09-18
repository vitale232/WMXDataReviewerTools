import datetime
import logging
import os
import sys
import time

import arcpy

sys.path.append(r'D:\Python')
import common


def run_batch_on_buffered_edits(reviewer_ws, reviewer_session, batch_job_file,
                                production_ws, production_ws_version,
                                logger=None, job__started_date=None, job__owned_by=None):
    if not logger:
        logger = common.initialize_logger(log_path=None, log_level=logging.INFO)

    logger.info(
        'Calling run_batch_on_buffered_edits(): Selecting edits by this user ' +
        'in this version and buffering by 10 meters'
    )

    arcpy.CheckOutExtension('datareviewer')

    # Call data reviewer batch job
    created_session = arcpy.CreateReviewerSession_Reviewer(
        reviewer_ws,
        reviewer_session
    )
    created_session_name = created_session.getOutput(0)
    logging.debug('Created new reviewer session: {}'.format(created_session_name))

    # Set the database connection as the workspace. All table and FC references come from here
    arcpy.env.workspace = production_ws

    # Check that the production workspace contains the production workspace version
    version_names = arcpy.ListVersions(production_ws)
    if not production_ws_version in version_names:
        raise AttributeError(
            'The version name \'{}\' does not exist in the workspace \'{}\'.'.format(
                production_ws_version,
                production_ws
            )
        )
    
    # Select the milepoint LRS feature class from the workspace
    milepoint_fc = [fc for fc in arcpy.ListFeatureClasses('*LRSN_Milepoint')]
    if len(milepoint_fc) != 1:
        raise ValueError(
            'Too many feature classes were selected while trying to find LRSN_Milepoint. ' + 
            'Selected FCs: {}'.format(milepoint_fc)
        )
    else:
        milepoint_fc = milepoint_fc[0]

    # Only set up a where_clause if the required inputs are present
    if not job__started_date or not job__owned_by:
        where_clause = None
    else:
        where_clause = 'EDITED_DATE >= \'{date}\' AND EDITED_BY = \'{user}\''.format(
            date=job__started_date,
            user=job__owned_by.split('\\')[1]
        )
    logging.debug('Using where_clause: {}'.format(where_clause))

    # Select Milepoint routes edited since the jobs creation by the job's owner
    sde_milepoint_layer = arcpy.MakeFeatureLayer_management(
        milepoint_fc,
        'milepoint_layer'
    )
    # Explicitly change version to the input version, as the SDE file could point to anything
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
    logging.debug('{count} features selected'.format(
            count=arcpy.GetCount_management(version_select_milepoint_layer).getOutput(0)
    ))

    # Create buffer polygons of the edited features. These will be used as the DR analysis_area
    logging.info('Buffering edited features by 10 meters')
    buffer_polygons = 'in_memory\\mpbuff_{}'.format(int(time.time()))
    arcpy.Buffer_analysis(
        version_select_milepoint_layer,
        buffer_polygons,
        '10 Meters'
    )

    reviewer_results = arcpy.ExecuteReviewerBatchJob_Reviewer(
        reviewer_ws,
        created_session_name,
        batch_job_file,
        production_workspace=production_ws,
        analysis_area=buffer_polygons,
        changed_features='ALL_FEATURES',
        production_workspaceversion=production_ws_version
    )

    logging.info('Data Reviewer completed with results: {}'.format(reviewer_results))
    with arcpy.da.SearchCursor(reviewer_results.getOutput(0), ["RECORDID","BATCHJOBFILE", "STATUS"]) as cursor:
        print( ["RECORDID","BATCHJOBFILE", "STATUS"])
        for row in cursor:
            print(row)

    try:
        arcpy.CheckInExtension('datareviewer')
        arcpy.Delete_management(buffer_polygons)
    except:
        pass

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
    production_ws_version = r'"SVC\AVITALE".HDS_GENERAL_EDITING_JOB_13242'

    # WMX inputs. Variable names represent WMX tokens.
    #  e.g. [JOB:STARTED_DATE] => job__started_date
    job__started_date = datetime.datetime(2019, 9, 16, 0, 0, 0)
    job__owned_by = r'SVC\AVITALE'


    run_batch_on_buffered_edits(
        reviewer_ws,
        reviewer_session,
        batch_job_file,
        production_ws,
        production_ws_version,
        logger=logger,
        job__started_date=job__started_date,
        job__owned_by=job__owned_by
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
