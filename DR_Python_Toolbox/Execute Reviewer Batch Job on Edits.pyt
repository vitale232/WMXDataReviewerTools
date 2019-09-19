import datetime
import logging
import os
import sys
import time
import traceback

import arcpy


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
        arcpy_messages.addGPMessages()
    else:
        raise ValueError('Parameter \'level\' must be one of (info, debug, error, gp)')

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
            'buffer polygons input as the Data Reviewer Analysis Area.'
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

        job__id_param = arcpy.Parameter(
            displayName='Workflow Manager Job ID',
            name='job__id',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

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

        log_path_param = arcpy.Parameter(
            displayName='Output Logfile Path',
            name='log_path',
            datatype='DETextfile',
            parameterType='Optional',
            direction='Output'
        )

        params = [
            reviewer_ws_param, batch_job_file_param, production_ws_param,
            job__id_param, job__started_date_param, job__owned_by_param,
            log_path_param
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
        batch_job_file = parameters[1].valueAsText
        production_ws = parameters[2].valueAsText
        job__id = parameters[3].valueAsText
        job__started_date = parameters[4].valueAsText
        job__owned_by = parameters[5].valueAsText
        log_path = parameters[6].valueAsText

        if log_path == '':
            log_path = None

        logger = initialize_logger(log_path=log_path, log_level=logging.DEBUG)

        self.run_batch_on_buffered_edits(
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

    def run_batch_on_buffered_edits(self, reviewer_ws, batch_job_file,
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
            sde_milepoint_layer = arcpy.MakeFeatureLayer_management(
                milepoint_fc,
                'milepoint_layer_{}'.format(int(time.time()))
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
                        level='debug', arcpy_messages=messages)
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
