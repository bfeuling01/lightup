# Lightup CTL

Lightup CTL is a CLI for Lightup Data auditing, management, and navigation

# Initializing

It is recommended that the user leverages python virtualenv for this setup.

1. Start by installing the required python libraries (requirements.txt).
2. Download the Lightup API file (lightup-api-credentials.json) from your Lightup instance
3. Place the API file (lightup-api-credentials.json) in the same folder location as the lightup_ctl.py file
4. Run using `python lightup_ctl.py`

# Usage

Lightup CTL will first load the Refresh Token and Server URL from the lightup-api-credentials.json file
that is in the same folder as the lightup_ctl.py file.

The user will be prompted to either run the Lightup Audit (Audit), explore their Lightup instance (Explore), or exit (EXIT).

The Audit function will look for all changes in Lightup within the last day, any orphaned metrics, and will output the results of
the audit into a Excel file in the same folder location as the python script
- An orphaned metric is any metric that is 24-hours or older and does not have an associated monitor

The Explore function will allow the user to explore Lightup based on their role permissions.
