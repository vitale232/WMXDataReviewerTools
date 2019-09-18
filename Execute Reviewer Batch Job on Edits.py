import datetime
import logging
import os
import sys
import time

import arcpy


def initialize_logger(log_path=None, log_level=logging.INFO):
    if log_path and not os.path.isdir(os.path.dirname(log_path)):
        os.makedirs(os.path.dirname(log_path))
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
        arcpy_messages.addMessage(message)
    elif level.lower() == 'debug':
        if logger:
            logging.debug(message)
        arcpy_messages.addMessage(message)
    elif level.lower() == 'error':
        if logger:
            logging.error(message)
        arcpy_messages.addErrorMessage(message)
    elif level.lower() == 'gp':
        arcpy_messages.addGPMessages()
    else:
        raise ValueError('\'level\' must be one of (info, debug, error, gp)')
    
    return True

class Toolbox(object):
    def __init__(self):
        self.label = 'NYSDOT Validations Toolbox'
        self.alias = 'revbatch'

        self.tools = [ExecuteReviewerBatchJobOnEdits]


class ExecuteReviewerBatchJobOnEdits(object):
    def __init__(self):
        self.label = 'Execute Reviewer Batch Job on R&H Edits'
        self.description = (
            'Determine which edits were made by the job\'s creator since the creation date, ' +
            'buffer the edits by 10 meters, and execute a Data Reviewer Batch Job with the ' +
            'buffer polygons determining the Data Reviewer Analysis Area'
        )
        self.canRunInBackground = False
    
    def getParameterInfo(self):
        reviewer_ws_param = arcpy.Parameter(
            displayName='Reviewer Workspace',
            name='reviewer_ws',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input'
        )

        reviewer_session_param = arcpy.Parameter(
            displayName='Reviewer Session Name',
            name='reviewer_session',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        batch_job_file_param = arcpy.Parameter(
            displayName='Reviewer Batch Job Filepath (.rbj)',
            name='batch_job_file',
            datatype='DEFile',
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

        production_ws_version_param = arcpy.Parameter(
            displayName='Production Workspace Version Name',
            name='production_ws_version',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        job__started_date_param = arcpy.Parameter(
            displayName='Edits Start Date (e.g. WMX Job Creation Date)',
            name='job__started_date',
            datatype='GPDate',
            parameterType='Optional',
            direction='Input'
        )

        job__owned_by_param = arcpy.Parameter(
            displayName='Editor Username (e.g. WMX Owned by User)',
            name='job__owned_by',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        log_path_param = arcpy.Parameter(
            displayName='Output Logfile Path',
            name='log_path',
            datatype='DETextfile',
            parameterType='Optional',
            direction='Output'
        )

        params = [
            reviewer_ws_param, reviewer_session_param, batch_job_file_param,
            production_ws_param, production_ws_version_param,
            job__started_date_param, job__owned_by_param, log_path_param
        ]

        return params
    
    def isLicensed(self):
        try:
            if arcpy.CheckExtension('datareviewer') != 'Available':
                raise Exception
        except Exception:
            return False

        return True
    
    def execute(self, parameters, messages):
        reviewer_ws = parameters[0].valueAsText
        reviewer_session = parameters[1].valueAsText
        batch_job_file = parameters[2].valueAsText
        production_ws = parameters[3].valueAsText
        production_ws_version = parameters[4].valueAsText
        job__started_date = parameters[5].valueAsText
        job__owned_by = parameters[6].valueAsText
        log_path = parameters[7].valueAsText

        if log_path == '':
            log_path = None
    
        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        self.run_batch_on_buffered_edits(
            reviewer_ws, reviewer_session, batch_job_file,
            production_ws, production_ws_version,
            job__started_date=job__started_date,
            job__owned_by=job__owned_by,
            logger=logger,
            messages=messages
        )

        return True

    def run_batch_on_buffered_edits(self, reviewer_ws, reviewer_session, batch_job_file,
                                    production_ws, production_ws_version,
                                    job__started_date=None, job__owned_by=None,
                                    logger=None, messages=None):
        if not logger:
            logger = initialize_logger(log_path=None, log_level=logging.INFO)

        log_it(('Calling run_batch_on_buffered_edits(): Selecting edits by this user ' +
                'in this version and buffering by 10 meters'),
                level='info', logger=logger, arcpy_messages=messages)

        arcpy.CheckOutExtension('datareviewer')

        # Call data reviewer batch job
        # created_session = arcpy.CreateReviewerSession_Reviewer(
        #     reviewer_ws,
        #     reviewer_session
        # )
        # created_session_name = created_session.getOutput(0)
        # log_it('Created new reviewer session: {}'.format(created_session_name),
        #        level='debug', logger=logger, arcpy_messages=messages)

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
        log_it('Using where_clause: {}'.format(where_clause),
               level='debug', logger=logger, arcpy_messages=messages)

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
        log_it('{count} features selected'.format(
               count=arcpy.GetCount_management(version_select_milepoint_layer).getOutput(0)),
               level='debug', logger=logger, arcpy_messages=messages)

        # Create buffer polygons of the edited features. These will be used as the DR analysis_area
        log_it('Buffering edited routes by 10 meters',
               level='info', logger=logger, arcpy_messages=messages)
        buffer_polygons = 'in_memory\\mpbuff_{}'.format(int(time.time()))
        arcpy.Buffer_analysis(
            version_select_milepoint_layer,
            buffer_polygons,
            '10 Meters'
        )
        log_it(level='gp', logger=logger, arcpy_messages=messages)

        reviewer_results = arcpy.ExecuteReviewerBatchJob_Reviewer(
            reviewer_ws,
            created_session_name,
            batch_job_file,
            production_workspace=production_ws,
            analysis_area=buffer_polygons,
            changed_features='ALL_FEATURES',
            production_workspaceversion=production_ws_version
        )
        log_it(level='gp', logger=logger, arcpy_messages=messages)
        log_it('Data Reviewer completed with results: {}'.format(reviewer_results),
               level='info', logger=logger, arcpy_messages=messages)
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
