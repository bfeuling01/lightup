# Lightup Create CLI

Lightup Create CLI is for creating and enabling metrics within Lightup

# Initializing

It is recommended that the user leverages python virtualenv for this setup.

1. Start by installing the required python libraries (requirements.txt).
2. Download the Lightup API file (lightup-api-credentials.json) from your Lightup instance
3. Place the API file (lightup-api-credentials.json) in the same folder location as the lightup_ctl.py file
4. Place the create_metric.xlsx file in the same folder location as the create_cli.py file
5. Update the create_metric.xlsx file with the correct information
6. Run using `python creare_cli.py`

# Usage

Lightup Create CLI will first load the Refresh Token and Server URL from the lightup-api-credentials.json file
that is in the same folder as the create_cli.py file.

The Create CLI will cycle through the Excel template, row by row, and update the required Lightup columns.

The Explore function will allow the user to explore Lightup based on their role permissions.

# Excel Template

In this repository there is an Excel Template with the required structure that the CLI will need
to run the creation process correctly. The 0.0.1 version of the Create CLI only handles NULL and 
DISTRIBUTION checks on columns where the Tables and Columns are enabled. Future versions of the CLI
will scale out further.
