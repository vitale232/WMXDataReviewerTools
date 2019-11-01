# Workflow Manager (WMX) Data Reviewer Tools
This repository contains ArcGIS geoprocessing tools and scripts to execute Esri Data Reviewer from Workflow Manager on a Roads and Highways ALRS. These validation tools are implemented using an ArcGIS Python Toolbox in ArcGIS Desktop 10.5.1. Esri does a nice job documenting [Python Toolboxes](http://desktop.arcgis.com/en/arcmap/10.5/analyze/creating-tools/a-quick-tour-of-python-toolboxes.htm).

## About the Tools
The WMXDataReviewerTools repository is the source code for the NYSDOT Validations Toolbox, which executes various Data Reviewer jobs against the Milepoint LRS. The validations and the code within this repository make strong assumptions regarding the schema of the data, thus this toolbox is not expected to "just run" on other datasets. However, it should serve as a fine example of some methods used to employ Data Reviewer in a Workflow Manager Workflow.

The toolbox consists of four tools: <br>
![Expanded toolbox screenshot](./docs/img/expanded_toolbox.PNG?raw=true "NYSDOT Validations Toolbox")

#### 1. Execute Reviewer Batch Job (RBJ) on R&H Edits
Selects the edits made by a specific user since a specific date (using the `EDITED_BY` and `EDITED_DATE` columns on LRSN_Milepoint), buffers the edits by 10 meters, and executes a specified Reviewer Batch Job file against all features that intersect the buffer polygons. This leads to the inclusion of routes that were not directly edited by the user in this given session in the RBJ execution.

![RBJ Buffer Intersection figure](./docs/img/intersection_map.PNG?raw=true "RBJ Feature Selection Diagram")
In the above figure, the yellow route was edited by the current user in the current WMX job. The red polygon represents the 10 meter buffer that is created while executing the Execute Reviewer Batch Job on R&H Tools from this toolbox. When this polygon is passed into the Execute Reviewer Batch Job geoprocessing tool, it will validate the yellow route plus all of the red routes in this diagram.

#### 2. Execute Roadway Level Attribute Validations
Selects the edits made by a specific user since a specific date (using the `EDITED_BY` and `EDITED_DATE` columns on LRSN_Milepoint) and runs them through a set of Python functions that validate specific data relationships. Examples include Local Roads should not have a `ROUTE_NUMBER`, `COUNTY_ORDER` should be a series of numbers incrementing by a value of one on a `DOT_ID`, the `ROUTE_SUFFIX` of a `ROADWAY_TYPE=Route` must be `None`, and many other validations. View the source code or the table below for a complete list.

#### 3. Execute SQL Validations Against Network
These validations are executed against the entire LRSN_Milepoint table, validating only active routes. There are two SQL queries that are executed as apart of this validation tool, one of which ensures there is only one combination of roadway-level attributes, the other of which ensures there is not more than one combination of `DOT_ID, COUNTY_ORDER, and DIRECTION`.

#### 4. Execute All validations
This tool serves as a wrapper function for items 1 through 3 of these lists. It executes the tools in this order: Execute Roadway Level Attribute Validations, Execute Reviewer Batch Job on R&H Edits, and Execute SQL Validations Against Network. The flow of data in this tool assumes this execution order.

# Execution
## Workflow Manager
This toolbox is mainly designed to run within a Workflow Manager 
The typical workflow will create a unique version for the user, create a corresponding Reviewer Workspace, launch ArcMap with the proper version and workspace so that the user can conduct their edits, launch this Python Toolbox as a geoprocessing tool, launch ArcMap again so the user can see the results of the validations, and finally close the version and the Workflow Manager job.

In NYSDOT's current workflow, the Execute All Validations tool is called. You can see the schematics of the workflow below:

![Workflow Manager Workflow screenshot](./docs/img/workflow_diagram.PNG?raw=true "Esri Workflow Manager (WMX) Workflow")


## Ad-Hoc
These tools can be used in an ad-hoc basis from within ArcGIS Desktop or ArcCatalog. It's important to understand the input parameters for the tool prior to execution, as they're designed to be pre-populated by Workflow Manager. Higher level documentation can be viewed in the tool help, but here's a brief description:

| Parameter | Description |
| ----------- | ----------- |
| job__started_date | The Milepoint feature class will be filtered with EDITED_DATE >= job__started_date |
| job__owned_by | The Milepoint feature class will be filtered with EDITED_BY = job__owned_by |
| job__id | The job__id will be used to construct the SDE version name for the edits. It assumes names like "SVC\{job__owned_by.upper()}".HDS_GENERAL_EDITING_JOB_{job__id}, where .upper() indicates all capital letters and job__id will correspond with the Workflow Manager Job ID |
| production_ws | SDE file pointing to the R&H geodatabase |
| reviewer_ws | SDE file or file geodatabase for the Data Reviewer results |
| log_path | If a log file is desired, it must have a .txt extension |
| log_level | DEBUG or INFO - This controls the Geoprocessing Tool's logging to the Arc Dialog and the output log file|
| batch_job_file | Path to a file with .rbj extension, which is a Reviewer Batch Job created in ArcMap |
| full_db_flag | If True, the filtering described in the first two rows is disregarded and all active features are validated |

# Validations
## Reviewer Batch Job
A [Reviewer Batch Job](https://desktop.arcgis.com/en/arcmap/10.5/extensions/data-reviewer/batch-jobs-and-data-reviewer.htm) is created using ArcGIS Desktop. Esri provides pre-programmed routines that search for hard-to-see errors in your data. Common examples are "dangles" (i.e. polylines that almost connect and probably were intended to connect) and "cutbacks" (i.e. the digitizer accidentally put a z shape into the polyline vertices).

The Reviewer Batch Job portion of the WMXDataReviewer Tools is the most malleable part of the validation workflow. Any user can go into ArcMap, load the correct data, and generate an RBJ to their specifications. The RBJ can then be fed into the tools via Workflow Manager or ad-hoc execution. Additionally, these tools are not required to run RBJ validations. However, Data Reviewer's Workflow Manager integration provides limited options for which features are input into an RBJ. Since running the RBJ on the entire LRS network is time consuming and overwhelms the user with results, this toolbox helps by selecting only the users edits. When the edits are buffered, the buffer polygon can be fed into the Data Reviewer Execute Reviewer Batch Job GP tool, which will select all intersecting features.

Here is a list of the checks that are currently included in the [RBJ](https://github.com/vitale232/WMXDataReviewerTools/blob/master/Reviewer_Batch_Jobs/RoutesInternalEventsValidations.rbj):

### Centerline Checks
| Check Title             | DR Check Type           | Where Clause              | Additional Parameters |
|-------------------------|-------------------------|---------------------------|-----------------------|
| Multipart Feature       | Multipart Feature       |                           |                       |
| Duplicate Vertices      | Duplicate Vertices      |                           | Tolerance: 0\.0011 meters |
| Overlapping Centerlines | Overlapping Centerlines |                           | FC1: Centerlines <br> FC2: Centerlines <br> Type: Overlap <br> Attributes: None |
| Invalid Geometry        | Invalid Geometry        |                           |                       |

### Calibration Point Checks
| Check Title        | DR Check Type    | Where Clause                                                                                                                | Additional Parameters |
|--------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------|-----------------------|
| Invalid FROM\_DATE | Execute SQL      | \(FROM\_DATE IS NULL\) OR \(FROM\_DATE < '01/01/2007'\) OR \(FROM\_DATE > TO\_DATE\) OR \(FROM\_DATE > CURRENT\_TIMESTAMP\) |
| Invalid TO\_DATE   | Execute SQL      | \(TO\_DATE < '01/01/2007'\) OR \(TO\_DATE < FROM\_DATE\) OR \(TO\_DATE > CURRENT\_TIMESTAMP\)                               |
| Invalid MEASURE    | Execute SQL      | \(MEASURE IS NULL\) OR \(MEASURE < 0\) OR \(MEASURE >= 100\)                                                                |
| Invalid Geometry   | Invalid Geometry |

### Milepoint Checks
| Check Title                 | DR Check Type                         | Where Clause                                                                                                                | Additional Parameters                                                                                                                                                       |
|-----------------------------|---------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Invalid Geometry            | Invalid Geometry                      | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       |
| Non\-Linear Segments        | Non\-Linear Segment                   |  \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       |
| Self\-Intersection Check    | Polyline or Path Closes on Self Check | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       |
| Domain Check                | Domain Check                          |  \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       |
| Duplicate Vertices          | Duplicate Vertices                    | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)        | Tolerance: 0\.0011 meters                                                                                                                                                   |
| Calibration Point on Routes | Geometry on Geometry                  | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       | FC1: Calibration Points <br> FC2: Milepoint <br> Tolerance: 0\.0011 meters <br> Compare Attributes: Milepoint\.ROUTE\_ID=Calibration\_Point\.ROUTE\_ID                      |
| Cutbacks                    | Cutbacks Check                        | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       | Min angle in degrees: 30                                                                                                                                                    |
| Length Check                | Evaluate Polyline Length Check        |  \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       | Type: Polyline <br> Length: <=0\.001 miles                                                                                                                                  |
| Dangles                     | Find Dangles                          | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       | Dangle Tolerance: 0\.009 miles                                                                                                                                              |
| Monotinicity Check          | Monotinicity Check                    | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)        | Evalueate: M values <br> Search Goal: Non\-monotonic features/Decreasing Values                                                                                             |
| Orphan Check                | Orphan Check                          |  \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       |
| Invalid FROM\_DATE          | Execute SQL                           | \(FROM\_DATE IS NULL\) OR \(FROM\_DATE < '01/01/2007'\) OR \(FROM\_DATE > TO\_DATE\) OR \(FROM\_DATE > CURRENT\_TIMESTAMP\) |
| Invalid TO\_DATE            | Execute SQL                           | \(TO\_DATE < '01/01/2007'\) OR \(TO\_DATE < FROM\_DATE\) OR \(TO\_DATE > CURRENT\_TIMESTAMP\)                               |
| Invalid FROM\_MEASURE       | Execute SQL                           | ROUND\(SHAPE\.STStartPoint\(\)\.M, 3\) <> 0\.000 OR ISNUMERIC\(SHAPE\.STStartPoint\(\)\.M\) <> 1                            |
| Invalid MEASURE\_RANGE      | Execute SQL                           | SHAPE\.STStartPoint\(\)\.M > SHAPE\.STEndPoint\(\)\.M                                                                       |
| Invalid TO\_MEASURE         | Execute SQL                           | SHAPE\.STEndPoint\(\)\.M IS NULL OR ISNUMERIC\(SHAPE\.STEndPoint\(\)\.M\) <> 1                                              |
| Too Many Vertices           | Execute SQL                           | SHAPE\.STNumPoints\(\) > 1500                                                                                               |
| Local Road Overlap          | Geometry on Geometry                  | ROADWAY\_TYPE = 1 AND TO\_DATE IS NULL                                                                                      | where\_clause: Same on both FCs <br> FC1: Milepoint <br> FC2: Milepoint <br> Spatial Relation Check type: Overlaps <br> Compare Attributes: CONC\_HIERARCHY=CONC\_HIERARCHY |
| Route Not on Centerline     | Geometry on Geometry                  | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\)       | where\_clause: FC1 blank <br> FC1: Centerlines <br> FC2: Milepoint <br> Spatial Relation Check Type: Within <br> Attributes: None                                           |

### Invalid Internal Event Checks
| Check Title                     | DR Check Type | Where Clause                                                                                                               | Additional Parameters                                                                                                                          |
|---------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| Functional Class                | Event Check   | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND <br> \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\) | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Municipality                    | Event Check   | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND <br> \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\) | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Maintenance Jurisdiction        | Event Check   | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND <br> \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\) | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Owning Jurisdiction             | Event Check   | \(FROM\_DATE IS NULL OR FROM\_DATE <= CURRENT\_TIMESTAMP\) AND <br> \(TO\_DATE IS NULL OR TO\_DATE >= CURRENT\_TIMESTAMP\) | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Number of Through Lanes Reverse | Event Check   | \(TO\_DATE IS NULL\) AND \(DIRECTION = '0' OR DIRECTION = '2'\)                                                            | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Number of Through Lanes Primary | Event Check   | \(TO\_DATE IS NULL\) AND \(DIRECTION = '0' OR DIRECTION = '1'\)                                                            | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Width of Through Lanes Reverse  | Event Check   | \(TO\_DATE IS NULL\) AND \(DIRECTION = '0' OR DIRECTION = '2'\)                                                            | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |
| Width of Through Lanes Primary  | Event Check   | \(TO\_DATE IS NULL\) AND \(DIRECTION = '0' OR DIRECTION = '1'\)                                                            | where\_clause: On event and Milepoint <br> Search Goals: Find orphans/Find overlaps/Find Gaps <br> Measure tolerance: 0\.000000621369949494949 |

## Roadway Level Attribute Checks
Roads and Highways provides dialog boxes for users to input roadway attributes. Many of these attributes are partially limited by the use of [Esri Attribute Domains](https://desktop.arcgis.com/en/arcmap/10.5/manage-data/geodatabases/an-overview-of-attribute-domains.htm). Domains do a good job of making sure the values lie within a specific set, however, they do not limit attributes based on the values of other attributes. Many of the fields in the LRSN_Milepoint table only apply to signed routes, which are indicated by `ROADWAY_TYPE=Route`. Other fields are assumed to be null when the `ROADWAY_TYPE=Road`.

The Roadway Level Attribute checks are managed in a Python function. The relevant features are read from the LRSN_Milepoint table using `arcpy.da.SearchCursor`, and the attributes and ROADWAY_TYPE are passed into the function. The function checks for the following relationships:

| Validation                                                    | Roadway Type                        |
|---------------------------------------------------------------|-------------------------------------|
| ROUTE\_ID must be a nine digit number                         | Road, Ramp, Route, or Non\-Mainline |
| DOT\_ID must be a six digit number                            | Road, Ramp, Route, or Non\-Mainline |
| COUNTY\_ORDER must be 2 digit zero padded                     | Road, Ramp, Route, or Non\-Mainline |
| COUNTY\_ORDER must be greater than 00                         | Road, Ramp, Route, or Non\-Mainline |
| COUNTY\_ORDER should be less than 29                          | Road, Ramp, Route, or Non\-Mainline |
| SIGNING must be null                                          | Road or Ramp                        |
| ROUTE\_NUMBER must be null                                    | Road or Ramp                        |
| ROUTE\_SUFFIX must be null                                    | Road or Ramp                        |
| ROUTE\_QUALIFIER must be 'No Qualifier'                       | Road or Ramp                        |
| PARKWAY\_FLAG must be No                                      | Road or Ramp                        |
| ROADWAY\_FEATURE must be null                                 | Road or Ramp                        |
| ROUTE\_NUMBER must not be null                                | Route                               |
| ROADWAY\_FEATURE must be null                                 | Route                               |
| ROUTE\_NUMBER must be '900' route when SIGNING is null        | Route                               |
| SIGNING must be null                                          | Non\-Mainline                       |
| ROUTE\_NUMBER must be null                                    | Non\-Mainline                       |
| ROUTE\_SUFFIX must be null                                    | Non\-Mainline                       |
| ROUTE\_QUALIFIER must be null                                 | Non\-Mainline                       |
| PARKWAY\_FLAG must be 'No'                                    | Non\-Mainline                       |
| ROADWAY\_FEATURE must not be null                             | Non\-Mainline                       |
| COUNTY\_ORDER must increment by a value of 1 for this DOT\_ID | Road, Ramp, Route, or Non\-Mainline |
| COUNTY\_ORDER must equal '01' for singular DOT\_ID            | Road, Ramp, Route, or Non\-Mainline |
| COUNTY\_ORDER has too many ROUTE\_IDs for this DOT\_ID        | Road, Ramp, Route, or Non\-Mainline |

## Execute SQL Validations
The SQL Validations check the full LRSN_Milepoint table for some essential relationships. While these validations are partially redundant, they provide the added benefit of always validating the entire active route network. This is extremely beneficial, as systems downstream of the R&H interface rely on sound data quality.

These validations are always run against a "Versioned View" of the data. The Esri SDE schema provides this database view so that you can execute SQL against different trees of the data. The version that the versioned view references is set with a stored database procedure. For example, you can see the Lockroot version in the versioned view by executing this SQL: `EXEC ELRS.sde.set_current_version "ELRS.Lockroot";`.

The first SQL function ensures that there is only one combination of roadway level attributes. This is essential since the LRSN is split at county borders. Thus, downstream routes can easily fall out of sync. It examines the attributes `SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, AND PARKWAY_FLAG`, while paying no mind to the DOT_ID, ROUTE_ID, and COUNTY_ORDER:
```SQL
SELECT DOT_ID, COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))
FROM ELRS.elrs.LRSN_Milepoint_evw
WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)
GROUP BY DOT_ID
HAVING COUNT (DISTINCT CONCAT(SIGNING, ROUTE_NUMBER, ROUTE_SUFFIX, ROADWAY_TYPE, ROUTE_QUALIFIER, ROADWAY_FEATURE, PARKWAY_FLAG))>1;
```

The second query ensures that there is only one combination of `DOT_ID, COUNTY_ORDER, DIRECTION`. The data model dictates that each DOT_ID corresponds with a roadway. For example, 100495 corresponds to I-87. The data model further dictates that each county border should see the roadways linear representation split, and, if the roadway continues into the next county, the `COUNTY_ORDER` should increment by a value of 1. Each combination of `DOT_ID and COUNTY_ORDER` could have up to 2 values for `DIRECTION`, which should correspond to both the Inventory (primary) and Non-Inventory (reverse) side of a dual carriageway highway. To test these assumptions are not exceeded, we use this query:
```SQL
SELECT DOT_ID, COUNTY_ORDER, DIRECTION, COUNT (1)
FROM ELRS.elrs.LRSN_Milepoint_evw
WHERE (FROM_DATE IS NULL OR FROM_DATE <= CURRENT_TIMESTAMP) AND (TO_DATE IS NULL OR TO_DATE >= CURRENT_TIMESTAMP)
GROUP BY DOT_ID, COUNTY_ORDER, DIRECTION
HAVING COUNT (1)>1
```

# Setup
## Workflow Manager Launch GP Tool Step Parameters
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
+ With much of the code living in a Python package called validation_helpers, it's hard to get ArcGIS to consistently update the tool. Just open a blank map document with the Data Reviewer extension activated whenever you'd like to test code changes.
+ VSCode is failing to correctly lint the validation_helpers package. It incorrectly identifies properly imported code. To workaround the issue, if you use the Open Folder function, open the ./NYSDOT_Validations_Toolbox directory instead of the project root or use a different IDE.
