import datetime
import logging
import os
import sys

import arcpy

sys.path.append(r'D:\Python')
import common


reviewer_db = r'D:\Validation\dev\2019-03-27_Dev.gdb'
session_name = 'arcpy : test 1'
reviewer_workspace = r''

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

logger.info('Script execution started at {}'.format(start_time))
logger.debug('Script: {}'.format(os.path.abspath(__file__)))

logger.debug('CheckOutExtension("datareviewer")')
arcpy.CheckOutExtension('datareviewer')

logger.info('Creating new Reviewer Session')
logger.debug('reviewer_db: {} | session_name: {}'.format(reviewer_db, session_name))
result = arcpy.CreateReviewerSession_Reviewer(reviewer_db, session_name)
new_session_name = result.getOutput(0)
logger.debug('Generated SessionID: {}'.format(new_session_name))



logger.debug('CheckInExtension("datareviewer")')
arcpy.CheckInExtension('datareviewer')
