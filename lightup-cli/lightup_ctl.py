##########################################################################################
# NAME: Lightup Control
# VERSION: 0.6.0
# DESCRIPTION:
# This Lightup CLI is intended for auditing and exploring purposes of Lightup
# which is based on the role associated to the API token supplied to the CLI.
#
# AUTHOR: Bryan "sneakyaneurism" Feuling
#
# NOTICE OF USAGE, RIGHTS, AND RESPONSIBILITY:
# This code is provided as an open source Lightup CLI and is not supported by Lightup.
# Lightup, or the author, are not responsible for any harm it may cause, including the
# unrecoverable corruption of a Lightup instance. It is recommended that modifications 
# to this code and production use by Lightup users only be done with the user's
# understanding as to what the code is doing and the intended outcome of the CLI.
#
##########################################################################################

import requests, json, inquirer, xlsxwriter as xl, concurrent.futures as cf, time
from datetime import datetime, timedelta

################## GET ACCESS TOKEN ##################
# This section gets the Refresh Token from the
# API Credential doc from Lightup and gets the
# access token for the rest of the CLI to use

f = open('./lightup-api-credential.json')
data = json.load(f)

REFRESH_TOKEN = str(data.get('data', {}).get('refresh'))
SERVER = str(data.get('data', {}).get('server'))
BASE_URL = f'{SERVER}/api/v1'

TOKEN_PAYLOAD = {"refresh": REFRESH_TOKEN}
TOKEN_URL = f'{BASE_URL}/token/refresh/'
TOKEN_RESPONSE = json.loads(requests.request("POST", TOKEN_URL, json=TOKEN_PAYLOAD).text)

ACCESS_TOKEN = 'Bearer ' + TOKEN_RESPONSE.get('access')

HEADERS = {
    "Authorization": ACCESS_TOKEN,
    "Accept": "application/json"
}

################## API REQUEST VALIDATION ##################
# This function is used to validate the API response
# to make sure the rest of the CLI can operate as expected.
# There is a check for a list or dictionary, and if the
# 'data' key exists in the resulting dictionary or not

def get_req_results(url):
    rs = json.loads(requests.request("GET", url, headers=HEADERS).text)
    if type(rs) == dict and rs != None:
        if rs.get('data') != None:
            return rs.get('data')
        else:
            return rs
    elif type(rs) == list:
        return rs
    else:
        return []

################## AUDITING ##################
# This function will execute a full audit
# of all changes for the past day and output
# the results in an Excel document for more
# exploration

def daily_audit(headers):
    print('RUNNING AUDIT')
    print('THIS CAN TAKE A FEW MINUTES')
    workspaces = {}
    LAST_DAY = (datetime.now() - timedelta(hours=24))
    TODAY_TMSP = datetime.today().timestamp()
    YESTERDAY_TMSP = (datetime.today() - timedelta(days = 1)).timestamp()
    WORKSPACES_AUDIT = []
    DATASOURCES_AUDIT = []
    EVENTS_AUDIT = []
    METRICS_AUDIT = []
    MONITORS_AUDIT = []
    ORPHANED_METRICS = []
    USERS_AUDIT = []
    DASHBOARDS_AUDIT = []
    INCIDENTS_AUDIT = []
    W_USERS_AUDIT = []
    
    #### VALIDATE TIMESTAMP
    def validate_time(x):
        if x is not None and datetime.fromtimestamp(x) >= LAST_DAY:
            return True
    
    #### GET APPLICATION USERS
    print('GETTING APPLICATION USER INFO')
    get_users_response = get_req_results(f'{SERVER}/api/v0/users')
    
    #### APPLICATION USER EVAL FUNCTION FOR MULTITHREADING
    def app_usr_eval(x):
        if validate_time(x.get('created_at')) == True:
            USERS_AUDIT.append(["NONE", "APP USER", "CREATION", str(x.get('username')), datetime.fromtimestamp(x.get('created_at')), "NONE"])
    
    with cf.ThreadPoolExecutor() as au_exec:
        app_users = [au_exec.submit(app_usr_eval, au) for au in get_users_response]
        for app_user in app_users:
            app_user.result()
    
    #### GET WORKSPACES INFORMATION
    print('GETTING WORKSPACE INFO')
    get_workspaces_response = get_req_results(f'{BASE_URL}/workspaces')
    
    #### WORKSPACE EVAL FUNCTION FOR MULTITHREADING
    def wksp_eval(x):
        workspaces[str(x.get('name'))] = str(x.get('uuid'))
        if validate_time(x.get('created_at')) == True:
        #### ADD WORKSPACE UUID FOR SCHEMA, TABLE, COLUMN, METRIC AUDITING
            WORKSPACES_AUDIT.append([str(x.get('name')), "WORKSPACE", "CREATION", str(x.get('name')), str(x.get('created_at')), "NONE"])
        
    with cf.ThreadPoolExecutor() as wksp_exec:
        wksp_results = [wksp_exec.submit(wksp_eval, wr) for wr in get_workspaces_response]
        for wksp_result in cf.as_completed(wksp_results):
            wksp_result.result()
            
    for wksp in workspaces:
        #### GET WORKSPACE USER INFORMATION
        print(f'GETTING USERS INFORMATION FOR {wksp}')
        get_wksp_users_response = get_req_results(f'{SERVER}/api/v0/ws/{workspaces[wksp]}/users')
        
        #### WORKSPACE USER EVAL FUNCTION FOR MULTITHREADING
        def user_eval(x):
            if validate_time(x.get('created_at')) == True:
                W_USERS_AUDIT.append([wksp, "WORKSPACE USER", "CREATION", str(x.get('username')), str(datetime.fromtimestamp(x.get('created_at'))), f"ROLE - {str(x.get('role'))}"])
        
        with cf.ThreadPoolExecutor() as user_exec:
            usr_results = [user_exec.submit(user_eval, wu) for wu in get_wksp_users_response]
            for usr_result in cf.as_completed(usr_results):
                usr_result.result()
            
        for wksp_user in get_wksp_users_response:
            user_eval(wksp_user)
            
        #### STORE MONITORS FOR METRIC COMPARISON
        monitors_set = set()
        
        #### GET WORKSPACE MONITOR INFORMATION
        print(f'GETTING MONITOR INFORMATION FOR WORKSPACE {wksp}')
        get_monitors_response = get_req_results(f'{BASE_URL}/ws/{workspaces[wksp]}/monitors')
        
        #### WORKSPACE MONITORS EVAL FUNCTION FOR MULTITHREADING
        def monitor_eval(x):
            mon_created = 0 if len(x) == 0 else int(x.get('status', {}).get('createdTs'))
            mon_updated = 0 if len(x) == 0 else int(x.get('status', {}).get('configUpdatedTs'))
            if mon_created >= mon_updated:
                if validate_time(x.get('status', {}).get('createdTs')) == True:
                    MONITORS_AUDIT.append([wksp, "MONITOR", "CREATED", str(x.get('metadata', {}).get('name')), str(datetime.fromtimestamp(x.get('status', {}).get('createdTs'))), str(x.get('metadata', {}).get('ownedBy', {}).get('username') or x.get('metadata', {}).get('updatedBy', {}).get('username'))])
            else:
                if validate_time(x.get('status', {}).get('configUpdatedTs')) == True:
                    MONITORS_AUDIT.append([wksp, "MONITOR", "UPDATED", str(x.get('metadata', {}).get('name')), str(datetime.fromtimestamp(x.get('status', {}).get('configUpdatedTs'))), str(x.get('metadata', {}).get('updatedBy', {}).get('username') or x.get('metadata', {}).get('ownedBy', {}).get('username'))])
            
            monitors_set.add(str(x.get('metadata', {}).get('uuid')))
        
        with cf.ThreadPoolExecutor() as mon_exec:
            mon_results = [mon_exec.submit(monitor_eval, mr) for mr in get_monitors_response]
            for mon_result in cf.as_completed(mon_results):
                mon_result.result()
        
        #### GET WORKSPACE METRIC INFORMATION
        print(f'GETTING METRIC INFORMATION FOR WORKSPACE {wksp}')
        get_metrics_response = get_req_results(f'{BASE_URL}/ws/{workspaces[wksp]}/metrics')
        
        #### WORKSPACE METRICS EVAL FUNCTION FOR MULTITHREADING
        def metric_eval(x):
            met_created = 0 if len(x) == 0 else int(x.get('status', {}).get('createdTs'))
            met_updated = 0 if len(x) == 0 else int(x.get('status', {}).get('configUpdatedTs'))
            if met_created >= met_updated:
                if validate_time(x.get('status', {}).get('createdTs')) == True:
                    MONITORS_AUDIT.append([wksp, "METRIC", "CREATED", str(x.get('metadata', {}).get('uuid')), str(datetime.fromtimestamp(x.get('status', {}).get('createdTs'))), str(x.get('metadata', {}).get('ownedBy', {}).get('username') or x.get('metadata', {}).get('updatedBy', {}).get('username'))])
            else:
                if validate_time(x.get('status', {}).get('configUpdatedTs')) == True:
                    MONITORS_AUDIT.append([wksp, "METRIC", "UPDATED", str(x.get('metadata', {}).get('uuid')), str(datetime.fromtimestamp(x.get('status', {}).get('configUpdatedTs'))), str(x.get('metadata', {}).get('updatedBy', {}).get('username') or x.get('metadata', {}).get('ownedBy', {}).get('username'))])
        
        #### WORKSPACE METRICS ORPHANED EVAL FUNCTION FOR MULTITHREADING
        def metric_orphaned(x):
            if x.get('metadata', {}).get('uuid') not in monitors_set:
                now = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                mdt = datetime.fromtimestamp(x.get('status', {}).get('createdTs'))
                diff = now - mdt
                hour_diff = (diff.days * 24) + (diff.seconds // 3600)
                if hour_diff > 24:
                    ORPHANED_METRICS.append([str(x.get('metadata', {}).get('uuid')), str(x.get('metadata', {}).get('ownedBy', {}).get('username') or x.get('metadata', {}).get('updatedBy', {}).get('username')), str(datetime.fromtimestamp(x.get('status', {}).get('createdTs')))])
        
        with cf.ThreadPoolExecutor() as met_exec:
            met_results = [met_exec.submit(metric_eval, me) for me in get_metrics_response]
            orph_results = [met_exec.submit(metric_orphaned, oe) for oe in get_metrics_response]
            for met_result in cf.as_completed(met_results):
                met_result.result()
            
            for orph_result in cf.as_completed(orph_results):
                orph_result.result()
                
        #### GET WORKSPACE INCIDENT INFORMATION
        print(f'GETTING INCIDENT INFORMATION FOR {wksp}')
        get_wksp_incident_response = get_req_results(f'{SERVER}/api/v0/ws/{workspaces[wksp]}/incidents/?start_ts={YESTERDAY_TMSP}&end_ts={TODAY_TMSP}')
        if len(get_wksp_incident_response) > 0:
            #### WORKSPACE INCIDENTS EVAL FUNCTION FOR MULTITHREADING
            def incident_eval(x):
                if validate_time(x.get('creation_ts')) == True:
                    INCIDENTS_AUDIT.append([wksp, "INCIDENT", "CREATED", str(x.get('id')), str(datetime.fromtimestamp(x.get('creation_ts'))), str(x.get('incident_type'))])
                
            with cf.ThreadPoolExecutor() as inc_exec:
                inc_results = [inc_exec.submit(incident_eval, wi) for wi in get_wksp_incident_response]
                for inc_result in cf.as_completed(inc_results):
                    inc_result.result()
        
        #### GET DATASOURCE INCIDENT INFORMATION
        print(f'GETTING DATASOURCE INFORMATION FOR {wksp}')
        get_datasources_response = get_req_results(f'{BASE_URL}/ws/{workspaces[wksp]}/sources')
        datasources = {}
        
        #### DATASOURCE EVAL FUNCTION FOR MULTITHREADING
        def source_eval(x):
            datasources[str(x.get('metadata', {}).get('name'))] = str(x.get('metadata', {}).get('uuid'))
            
            #### CHECK FOR CREATED OR UPDATED IN THE LAST DAY
            s_created = 0 if len(x) == 0 else int(x.get('status', {}).get('createdTs'))
            s_updated = 0 if len(x) == 0 else int(x.get('status', {}).get('configUpdatedTs'))
            
            if s_created >= s_updated:
                if validate_time(x.get('status', {}).get('createdTs')) == True:
                    DATASOURCES_AUDIT.append([wksp, "DATASOURCE", "CREATION", str(x.get('metadata', {}).get('name')), str(x.get('status', {}).get('createdTs')), str(x.get('metadata', {}).get('ownedBy', {}).get('username'))])
            else:
                if validate_time(x.get('status', {}).get('configUpdatedTs')) == True:
                    DATASOURCES_AUDIT.append([wksp, "DATASOURCE", "UPDATE", str(x.get('metadata', {}).get('name')), str(x.get('status', {}).get('configUpdatedTs')), str(x.get('metadata', {}).get('updatedBy', {}).get('username'))])
        
        with cf.ThreadPoolExecutor() as src_exec:
            src_results = [src_exec.submit(source_eval, sr) for sr in get_datasources_response]
            for src_result in cf.as_completed(src_results):
                src_result.result()
        
        for d in datasources:
            print(f'GETTING DATASOURCE CHANGES FOR WORKSPACE {wksp} AND DATASOURCE {d}')
            get_wksp_events = get_req_results(f'{SERVER}/api/v0/ws/{workspaces[wksp]}/sources/{datasources[d]}/profile/events/?start_ts={YESTERDAY_TMSP}&end_ts={TODAY_TMSP}')
            if len(get_wksp_events) > 0:
                def event_eval(x):
                    EVENTS_AUDIT.append([wksp, "EVENT", str(x.get('eventType')).upper(), (x.get('eventDetail', {}).get('msg')).split(' ')[1], str(datetime.fromtimestamp(x.get('eventTs'))), "NONE"])
                
                with cf.ThreadPoolExecutor() as event_exec:
                    event_results = [event_exec.submit(event_eval, ee) for ee in get_wksp_events]
                    for event_result in cf.as_completed(event_results):
                        event_result.result()
            

    #### AUDIT LIST ARRAY FOR CSV CREATION
    AUDIT_LIST = []
    
    #### CHECK FOR WHICH AUDIT SETS NEED TO BE ADDED TO CSV
    if len(WORKSPACES_AUDIT) > 0:
        for wa in WORKSPACES_AUDIT:
            AUDIT_LIST.append(wa)
    if len(DATASOURCES_AUDIT) > 0:
        for da in DATASOURCES_AUDIT:
            AUDIT_LIST.append(da)
    if len(EVENTS_AUDIT) > 0:
        for sa in EVENTS_AUDIT:
            AUDIT_LIST.append(sa)
    if len(METRICS_AUDIT) > 0:
        for meta in METRICS_AUDIT:
            AUDIT_LIST.append(meta)
    if len(MONITORS_AUDIT) > 0:
        for mona in MONITORS_AUDIT:
            AUDIT_LIST.append(mona)
    if len(USERS_AUDIT) > 0:
        for ua in USERS_AUDIT:
            AUDIT_LIST.append(ua)
    if len(W_USERS_AUDIT) > 0:
        for wua in W_USERS_AUDIT:
            AUDIT_LIST.append(wua)
    if len(DASHBOARDS_AUDIT) > 0:
        for dasha in DASHBOARDS_AUDIT:
            AUDIT_LIST.append(dasha)
    if len(INCIDENTS_AUDIT) > 0:
        for ia in INCIDENTS_AUDIT:
            AUDIT_LIST.append(ia)
    
    #### CREATE EXCEL DOC FOR AUDIT OUTPUT
    with xl.Workbook("./audit_output.xlsx") as workbook:
        #### ADD NEW DATA
        if len(AUDIT_LIST) > 0:
            fields = ["WORKSPACE", "OBJECT", "EVENT", "OBJECT NAME", "EVENT TIME", "NAME"]
            row = 1
            audit_worksheet = workbook.add_worksheet('AUDIT')
            audit_worksheet.write_row(0, 0, fields)
            for al in AUDIT_LIST:
                audit_worksheet.write_row(row, 0, al)
                row += 1
        
        #### ADD ORPHANED METRIC INFORMATION TO NEW EXCEL WORKSHEET
        if len(ORPHANED_METRICS) > 0:
            fields = ["METRIC UUID", "CREATED BY", "CREATED AT"]
            row = 1
            orphaned_worksheet = workbook.add_worksheet('ORPHANS')
            orphaned_worksheet.write_row(0, 0, fields)
            for om in ORPHANED_METRICS:
                orphaned_worksheet.write_row(row, 0, om)
                row += 1

################## GET APPLICATION USERS ###################
# APPLICATION USERS ARE LISTED
# PRINTS USERNAME
# PRINTS EMAIL
# PRINTS NAME
# PRINTS CREATED AT
# PRINTS LAST LOGIN
# PRINTS APPLICATION ROLE

def get_app_users_info(headers):
    users_url = f'{SERVER}/api/v0/users'
    get_users_response = json.loads(requests.request("GET", users_url, headers=headers).text)
    for u in get_users_response:
        print(f"""
User: {str(u.get('username'))}
Email: {str(u.get('email'))}
Name: {str(u.get('first_name'))} {str(u.get('last_name'))}
Created At: {str(datetime.fromtimestamp(u.get('created_at')))}
Last Login: {str(datetime.fromtimestamp(u.get('last_login')))}
Application Role: {str(u.get('app_role'))}
""")
        
################## GET WORKSPACE USERS ##################
# WORKSPACE USERS ARE LISTED
# PRINTS USERNAME
# PRINTS EMAIL
# PRINTS NAME
# PRINTS CREATED AT
# PRINTS LAST LOGIN
# PRINTS APPLICATION ROLE
def get_wksp_users_info(headers, workspace):
    users_list_url = f'{SERVER}/api/v0/ws/{workspace}/users'
    get_users_response = json.loads(requests.request("GET", users_list_url, headers=headers).text)
    for u in get_users_response:
        print(f"""
User: {str(u.get('username'))}
Email: {str(u.get('email'))}
Name: {str(u.get('first_name'))} {str(u.get('last_name'))}
Created At: {str(datetime.fromtimestamp(u.get('created_at')))}
Last Login: {str(datetime.fromtimestamp(u.get('last_login')))}
Workspace Role: {str(u.get('role'))}
""")

################## GET WORKSPACE ##################
# USER SELECTS WORKSPACE FROM LIST
# PRINTS WORKSPACE NAME
# PRINTS WORKSPACE DESCRIPTION
# PRINTS WORKSPACE CREATE DATE
# GETS DATASOURCE INFORMATION

def get_workspace_info(headers):
    workspaces_url = f'{BASE_URL}/workspaces'
    
    ### Create Empty dict for workspace results
    workspaces = {}

    get_workspaces_response = json.loads(requests.request("GET", workspaces_url, headers=headers).text)

    ### Add workspace Name and UUID to dict
    for w in get_workspaces_response.get('data'):
        workspaces[w.get('name')] = w.get('uuid')
    
    ### Add EXIT row to dict
    workspaces['USERS'] = 'USERS'
    workspaces['EXIT'] = 'EXIT'

    ### Ask user for desired results
    workspace_list = [
        inquirer.List(
            "wksp",
            message = "Which Workspace?",
            choices = workspaces.keys(),
        ),
    ]

    ### Get user input
    workspace_choice = inquirer.prompt(workspace_list)
    workspace = workspaces.get(workspace_choice.get('wksp'))
    
    ### Exit route
    if workspace_choice.get('wksp') == 'EXIT':
        exit()
    
    if workspace_choice.get('wksp') == 'USERS':
        get_app_users_info(HEADERS)
        workspace_choice = inquirer.prompt(workspace_list)
        workspace = workspaces.get(workspace_choice.get('wksp'))
    else:
        ### Print Workspace details
        workspace_url = f'{BASE_URL}/workspaces/{workspace}'
        
        for d in json.loads(requests.request("GET", workspaces_url, headers=headers).text).get('data'):
            if workspace_choice.get('wksp') == d.get('name'):
                print(f"""
Workspace Name: {str(d.get('name'))}
Workspace Description: {str(d.get('description'))}
Created at: {str(datetime.fromtimestamp(d.get('created_at')))}
""")
        
        ### Get a list of datasources in the workspace for the user
        get_datasource_info(HEADERS, workspace)
            
        return workspace, workspace_url
    
################## GET DATASOURCE ##################
# USER SELECTS DATASOURCE FROM LIST
# PRINTS DATASOURCE DETAILS
# GETS SCHEMA INFORMATION

def get_datasource_info(headers, workspace):
    datasources_list_url = f'{BASE_URL}/ws/{workspace}/sources'
    
    ### Create an empty dict for later
    datasources = {}

    get_datasources_response = json.loads(requests.request("GET", datasources_list_url, headers=headers).text)

    ### Fill in the dict
    for d in get_datasources_response:
        datasources[d.get('metadata', {}).get('name')] = d.get('metadata', {}).get('uuid')

    ### Add an EXIT route to the dict
    datasources['USERS'] = 'USERS'
    datasources['EXIT'] = 'EXIT'
    
    ### Show selection of datasources
    source_list = [
        inquirer.List(
            "ds",
            message = "Which Datasource?",
            choices = datasources.keys(),
        ),
    ]

    ### Get user response
    source_choice = inquirer.prompt(source_list)
    
    datasource = datasources.get(source_choice.get('ds'))
    
    ### Exit route
    if source_choice.get('ds') == 'EXIT':
        exit()
        
    if source_choice.get('ds') == 'USERS':
        get_wksp_users_info(headers, workspace)

    ### Datasource URL to pass to the Get Details function
    datasource_url = f'{datasources_list_url}/{datasource}'
    
    ### Get datasource details
    datasource_details = json.loads(requests.request("GET", datasource_url, headers=headers).text)
    ds_type = datasource_details.get('config', {}).get('connection', {}).get('type')
    
    ### Print generic datasource details
    created = ('None' if datasource_details.get('status', {}).get('createdTs') is None else datetime.fromtimestamp(datasource_details.get('status', {}).get('createdTs')))
    scanned = ('None' if datasource_details.get('status', {}).get('createdTs') is None else datetime.fromtimestamp(datasource_details.get('status', {}).get('lastScannedTs')))
    print(f"""
Datasource Name: {str(datasource_details.get('metadata', {}).get('name'))}
Datasource Type: {str(datasource_details.get('config', {}).get('connection', {}).get('type'))}
Owner: {str(datasource_details.get('metadata', {}).get('ownedBy', {}).get('email', datasource_details.get('metadata', {}).get('updatedBy', {}).get('email')))}
Created At: {str(created)}
Profiling Enabled: {str(datasource_details.get('config', {}).get('isLive'))}
Datasource Tags: {str(datasource_details.get('metadata', {}).get('tags'))}""")
    if str(datasource_details.get('status', {}).get('lastScannedStatus')) == 'success':
        print("Successful Last Scan: " + str(scanned))
    else:
        print(f"""
Last Scan Failed At: {str(scanned)}
Last Scan Fail Reason: {str(datasource_details.get('status', {}).get('lastScannedFailedReason'))}""")

    if ds_type == 'postgres':
        print(f"""Postgres Database Name: {str(datasource_details.get('config', {}).get('connection', {}).get('dbname'))}
Postgres Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
Postgres Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
Postgres Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    elif ds_type == 'athena':
        print(f"""Athena Region: {str(datasource_details.get('config', {}).get('connection', {}).get('regionName'))}
S3 Staging Dir: {str(datasource_details.get('config', {}).get('connection', {}).get('s3StagingDir'))}
Postgres Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
Postgres Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    elif ds_type == 'databricks':
        print(f"""Databricks Workspace: {str(datasource_details.get('config', {}).get('connection', {}).get('workspaceUrl'))}
Databricks Workspace: {str(datasource_details.get('config', {}).get('connection', {}).get('workspaceUrl'))}
            """)
    elif ds_type == 'microsoftsql':
        print(f"""MSSQL Database Name: {str(datasource_details.get('config', {}).get('connection', {}).get('dbname'))}
MSSQL Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
MSSQL Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
MSSQL Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    elif ds_type == 'oracle':
        print(f"""Oracle Database Name: {str(datasource_details.get('config', {}).get('connection', {}).get('dbname'))}
Oracle Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
Oracle Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
Oracle Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    elif ds_type == 'redshift':
        print(f"""Redshift Database Name: {str(datasource_details.get('config', {}).get('connection', {}).get('dbname'))}
Redshift Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
Redshift Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
Redshift Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
        print("\n")
    elif ds_type == 'snowflake':
        print(f"""Snowflake Database Name: {datasource_details.get('config', {}).get('connection', {}).get('dbname')}
Snowflake Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
Snowflake Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    elif ds_type == 'bigquery':
        print(f"""
            """)
    elif ds_type == 'incorta':
        print(f"""Incorta Database Name: {datasource_details.get('config', {}).get('connection', {}).get('dbname')}
Incorta Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
Incorta Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
Incorta Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    elif ds_type == 'teradata':
        print(f"""Teradata Host: {str(datasource_details.get('config', {}).get('connection', {}).get('host'))}
Teradata Port: {str(datasource_details.get('config', {}).get('connection', {}).get('port'))}
Teradata Username: {str(datasource_details.get('config', {}).get('connection', {}).get('user'))}
            """)
    else: print(datasource_details)
    
    schema_url = f'{datasource_url}/profile/schemas'
    schemas = json.loads(requests.request("GET", schema_url, headers=headers).text)
    
    disabled_schemas = {}
    enabled_schemas = {}
    
    for s in schemas.get('data'):
        if s.get('profilerConfig', {}).get('enabled') == False:
            disabled_schemas[s.get('name')] = s.get('uuid')
        else:
            enabled_schemas[s.get('name')] = s.get('uuid')
    
    print('''SCHEMAS WITH PROFILING:
''')
    for e in enabled_schemas:
        print(e)
    print('''
SCHEMAS WITHOUT PROFILING:
''')
    for d in disabled_schemas:
        print(d)
    print('')
    
    next_steps = [
        inquirer.List(
            "next",
            message = "What would you like to do next?",
            choices = ['Schema Details', 'View Tables', 'EXIT'],
        ),
    ]
    
    nxt = inquirer.prompt(next_steps)
    if nxt.get('next') == 'EXIT':
        exit()

    if nxt.get('next') == 'View Tables':
        if len(enabled_schemas) == 1:
            print(list(enabled_schemas.keys())[0])
            get_table_info(HEADERS, datasource_url, list(enabled_schemas.keys())[0])
        else:
            enabled_schemas['EXIT'] = ['EXIT']
            get_schema = [
                inquirer.List(
                    "schema",
                    message = "What Schema do you want to use?",
                    choices = enabled_schemas.keys(),
                ),
            ]
            sch = inquirer.prompt(get_schema)
            get_table_info(HEADERS, datasource_url, sch.get('schema'))
    
    if nxt.get('next') == 'Schema Details':
        for v in enabled_schemas.values():
            schema_details = json.loads(requests.request("GET", f'{schema_url}/{v}', headers=headers).text)
            fs = str(datetime.fromtimestamp(schema_details.get('firstSeenTs'))) if schema_details.get('firstSeenTs') != None else "Never Seen"
            lse = str(datetime.fromtimestamp(schema_details.get('lastSeenTs'))) if schema_details.get('lastSeenTs') != None else "Never Seen"
            rem = str(datetime.fromtimestamp(schema_details.get('removedTs'))) if schema_details.get('removedTs') != None else "Still Live"
            lts = str(datetime.fromtimestamp(schema_details.get('lastTablesScannedTs'))) if schema_details.get('lastTablesScannedTs') != None else "Never Scanned"
            lsc = str(datetime.fromtimestamp(schema_details.get('lastScannedTs'))) if schema_details.get('lastScannedTs') != None else "Never Scanned"
            print(f"""
Schema Name: {str(schema_details.get('name'))}
First Seen: {fs}
Last Seen: {lse}
Removed: {rem}
Last Scanned: {lsc}
Last Table Scannin: {lts}

SCHEMA MONITORING
Schema Change: {str(schema_details.get('profilerConfig', {}).get('tableListChange', {}).get('enabled'))}
Schema Change Alerts: {str(schema_details.get('profilerConfig', {}).get('tableListChange', {}).get('monitoring', {}).get('enabled'))}
""")
    
    get_table_details = [
        inquirer.List(
            "tables",
            message = "What would you like to do next?",
            choices = ['View Tables', 'EXIT'],
        ),
    ]
    
    gtd = inquirer.prompt(get_table_details)
    
    if gtd.get('tables') == 'EXIT':
        exit()
    
    if gtd.get('tables') == 'View Tables':
        if len(enabled_schemas) == 1:
            print(list(enabled_schemas.keys())[0])
            get_table_info(HEADERS, datasource_url, list(enabled_schemas.keys())[0])
        
    return workspace, datasource, datasource_details, datasource_url

def get_table_info(headers, datasource_url, schema):

    tables_list_url = f'{datasource_url}/tables'

    enabled_tables = {}
    disabled_tables = []

    get_tables_response = json.loads(requests.request("GET", tables_list_url, headers=headers).text)

    for t in get_tables_response:
        if t.get('schemaName') == schema and t.get('profilerConfig', {}).get('enabled') == True:
            enabled_tables[t.get('tableName')] = t.get('tableUuid')
        elif t.get('schemaName') == schema and t.get('profilerConfig', {}).get('enabled') == True:
            disabled_tables.append(t.get('tableName'))
    
    enabled_tables['Unmonitored Tables'] = ['Unmonitored Tables']
    enabled_tables['EXIT'] = ['EXIT']

    tables_list = [
        inquirer.List(
            "tb",
            message = "View Monitored Table or See Unmonitored Tables?",
            choices = enabled_tables.keys(),
        ),
    ]

    table_choice = inquirer.prompt(tables_list)
    table = enabled_tables.get(table_choice.get('tb'))
    
    if table_choice.get('tb') == 'EXIT':
        exit()
    
    if table_choice.get('tb') == 'Unmonitored Tables':
        if len(disabled_tables) > 0:
            print(disabled_tables)
        else:
            print('All Tables are Monitored')
            exit()

    table_url = f'{datasource_url}/profile/tables/{table}'
    table_details = json.loads(requests.request("GET", table_url, headers=headers).text)
    
    coll_sched = "Scheduled" if table_details.get('profilerConfig', {}).get('triggered') == False else "Triggered"
    query_scope = "Incremental" if table_details.get('profilerConfig', {}).get('queryScope') == 'timeRange' else "Full Table"
    partitioning = list(table_details.get('profilerConfig', {}).get('partitions')) if len(table_details.get('profilerConfig', {}).get('partitions')) > 0 else "No Partitions"
    partitionTZ = "No Partitions" if partitioning == "No Partitions" else str(table_details.get('profilerConfig', {}).get('partitionTimezone'))

    print(f"""
TABLE OVERVIEW
Schema: {str(table_details.get('schemaName'))}
Table: {str(table_details.get('tableName'))}
Last Seen: {str(datetime.fromtimestamp(table_details.get('lastSeenTs')))}
Last Scanned: {str(datetime.fromtimestamp(table_details.get('lastScannedTs')))}
Query Scope: {query_scope}
Collection Type: {coll_sched}
Timestamp Column: {str(table_details.get('profilerConfig', {}).get('timestampColumn'))}
Timestamp Timezone: {str(table_details.get('profilerConfig', {}).get('timezone'))}
Aggregation Interval: {str(table_details.get('profilerConfig', {}).get('window'))}
Aggregation Timezone: {str(table_details.get('profilerConfig', {}).get('dataTimezone'))}
Evaluation Delay: {str(table_details.get('profilerConfig', {}).get('syncDelay')/60)} minutes
Partitions: {partitioning}
Partition Timezone: {partitionTZ}

TABLE LEVEL MONITORING
Data Delay: {str(table_details.get('profilerConfig', {}).get('dataDelay', {}).get('enabled'))}
Data Volume: {str(table_details.get('profilerConfig', {}).get('volume', {}).get('enabled'))}

TABLE STATUS
Data Delay Last Check: {str(datetime.fromtimestamp(table_details.get('status', {}).get('dataDelay', {}).get('lastEventTs')))}
Data Volume Last Check: {str(datetime.fromtimestamp(table_details.get('status', {}).get('tableVolume', {}).get('lastEventTs')))}
""")
    
    next_steps = [
        inquirer.List(
            "next",
            message = "Columns or EXIT?",
            choices = ['Columns', 'EXIT'],
        ),
    ]
    
    nxt = inquirer.prompt(next_steps)
    if nxt.get('next') == 'EXIT':
        exit()
    if nxt.get('next') == 'Columns' :
        get_column_info(HEADERS, table_url)
        
    return table, table_details, table_url

def get_column_info(headers, table_url):
    columns_list_url = f'{table_url}/columns'
    columns = {}

    get_columns_response = json.loads(requests.request("GET", columns_list_url, headers=headers).text)

    for c in get_columns_response:
        columns[c.get('columnName')] = c.get('uuid')

    column_list = [
        inquirer.List(
            "col",
            message = "Which Column?",
            choices = columns.keys(),
        ),
    ]
    column_choice = inquirer.prompt(column_list)
    column = columns.get(column_choice.get('col'))

    column_url = f'{columns_list_url}/{column}'

    get_column_info = json.loads(requests.request("GET", column_url, headers=headers).text)

    print(f"""
Column Name: {str(get_column_info.get('columnName'))}
Data Type: {str(get_column_info.get('columnType'))}
Categorical Distribution Enabled: {str(get_column_info.get('profilerConfig', {}).get('categoricalDistribution', {}). get('enabled'))}
Numerical Distribution Enabled: {str(get_column_info.get('profilerConfig', {}).get('numericalDistribution', {}).get('enabled'))}
Null% Enabled: {str(get_column_info.get('profilerConfig', {}).get('missingValue', {}).get('enabled'))}
""")

if __name__ == '__main__':
    initial_action = [
        inquirer.List(
            "init",
            message = "Audit or Explore?",
            choices = ['Audit', 'Explore', 'EXIT'],
        )
    ]
    
    init_choice = inquirer.prompt(initial_action)
    if init_choice.get('init') == 'EXIT':
        exit()
    elif init_choice.get('init') == 'Audit':
        daily_audit(HEADERS)
    else:
        get_workspace_info(HEADERS)
