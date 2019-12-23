# Files in ./src Directory

### ./NYSDOT Validations Toolbox.pyt
This is the main executable file of the source code. It is an ArcGIS Python Toolbox that executes the Data Reviewer and SQL validations 
from the ArcGIS environment (e.g. ArcGIS Desktop, Workflow Manager, etc). The tool is written in Python, but given a .pyt file extension
rather than the typical .py associated with Python. This lets ArcGIS know it should expect a certain class structure, which provides
ArcGIS the expectations for the GUI and execution environment.

### NYSDOT Validations Toolbox.pyt.xml
This XML file is the documentation that's associated with the Python Toolbox. The docs are edited in ArcGIS Desktop. This file should 
be included with the tool, so that the interactive help dialogs of the Geoprocessing Tool behave as expected.

### NYSDOT Validations Toolbox.ExecuteAllValidations.pyt.xml
This XML file is the documentation for the Execute All Validations tool within the toolbox. The docs are edited in ArcGIS Desktop.
This file should be included with the tool, so that the interactive help dialogs of the GP tool behave as expected.

### NYSDOT Validations Toolbox.ExecuteNetworkSQLValidations.pyt.xml
This XML file is the documentation for the Execute Network SQL Validations tool within the toolbox. The docs are edited in ArcGIS Desktop.
This file should be included with the tool, so that the interactive help dialogs of the GP tool behave as expected.

### NYSDOT Validations Toolbox.ExecuteReviewerBatchJobOnEdits.pyt.xml
This XML file is the documentation for the Execute Reviewer Batch Job on Edits tool within the toolbox. The docs are edited in ArcGIS Desktop.
This file should be included with the tool, so that the interactive help dialogs of the GP tool behave as expected.

### NYSDOT Validations Toolbox.ExecuteRoadwayLevelAttributeValidatio
This XML file is the documentation for the Execute Roadway Level Attributes tool within the toolbox. The docs are edited in ArcGIS Desktop.
This file should be included with the tool, so that the interactive help dialogs of the GP tool behave as expected.

# Directories in ./src Directory

### ./.vscode
This directory contains a `settings.json` file that can be used to configure VS Code with the correct Python executable and other settings.

### ./validation_helpers
This directory contains a Python module that is used by the `./NYSDOT Validations Toolbox.pyt` to conduct the Roads and Highways validations. 
The module includes utilities to query the data, query the underlying infrastructure (e.g. Data Reviewer session tables), run the validations, 
and write the results to the Data Reviewer Table.
