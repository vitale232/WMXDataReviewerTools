# WMXDataReviewerTools
ArcGIS geoprocessing tools and scripts to execute Esri Data Reviewer from Workflow Manager on a Roads and Highways ALRS. These validation tools are implemented using an ArcGIS Python Toolbox in ArcGIS Desktop 10.5.1. Esri does a nice job documenting [Python Toolboxes](http://desktop.arcgis.com/en/arcmap/10.5/analyze/creating-tools/a-quick-tour-of-python-toolboxes.htm).

# Setup
## Workflow Manager Launch GP Tool Parameters
| Parameter | Value |
| ----------- | ----------- |
| job__started_date | [JOB:STARTED_DATE] |
| job__owned_by | [JOB:OWNED_BY] |
| job__id | [JOB:ID] |
| production_ws | Database Connections\dev_elrs_ad_Lockroot.sde |
| reviewer_ws | Database Connections\dev_elrs_datareviewer_dr_user.sde |
| log_path |  |
| log_level | DEBUG |
| batch_job_file | P:\Office of Engineering\Technical Services\Highway Data Services Bureau\GIS\Roads_And_Highways_Tools\WMXDataReviewerTools\Reviewer_Batch_Jobs\RoutesInternalEventsValidations.rbj |
| full_db_flag |  |

*The parameters of the form [JOB:FOOBAR] refer to [Workflow Manager Tokens](https://desktop.arcgis.com/en/arcmap/10.5/extensions/workflow-manager/tokens.htm)*

## Development Notes
+ With much of the code living in a Python package, it's hard to get ArcGIS to consistently update the tool. Just open a blank map document with the Data Reviewer extension activated whenever you'd like to test code changes.
