# Lightup Metric Trigger

This CLI is used to trigger metrics

# Initializing

It is recommended that the user leverages python virtualenv for this setup.

1. Start by installing the required python libraries (requirements.txt).
2. Download the Lightup API file (lightup-api-credentials.json) from your Lightup instance
3. Place the API file (lightup-api-credentials.json) in the same folder location as the metric_trigger.py file
4. Run using `python metric_trigger.py`

# Usage

Metric Trigger CLI will first load the Refresh Token and Server URL from the lightup-api-credentials.json file
that is in the same folder as the lightup_ctl.py file.

The user will be prompted to select a desired Lightup Workspace.

Then, the user will be asked to either trigger individual metrics (METRICS), all triggerable metrics in a table (TABLES), or exit (EXIT).

Finally, the user will be given a list of available TABLES or METRICS to trigger. The user can select as many
items from the list using the SPACEBAR to select and ENTER to complete the selection

# TODO

Incorporate the trigger CLI into the main Lightup CLI
