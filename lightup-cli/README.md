# Lightup CTL

Lightup CTL is a CLI for Lightup Data auditing, management, and navigation

# Initializing

It is recommended that the user leverages python virtualenv for this setup.

1. Start by installing the required python libraries (requirements.txt).
2. Add your Lightup URL to the top of the CLI code (this will be changed to a creds file later)
3. Add your Lightup API key to the top of the CLI code (this will be changed to a creds file later)
4. Save the python file and run using `python lightup_ctl.py`

# Usage

Lightup CTL will start by looking through which workspace the API key has access to.

The user will have menu options to navigate through their Lightup setup. Currently, these
options are limited to general auditing of Lightup. In the future, users will be able to take
actions within Lightup through the CLI.
