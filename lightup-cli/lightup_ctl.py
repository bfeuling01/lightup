import requests, json, inquirer, xlsxwriter as xl
from datetime import datetime, timedelta

################## GET ACCESS TOKEN ##################
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

def daily_audit(headers):
    #### AUDIT_LIST DICT IS FOR AUDIT READOUT
    #### WORKSPACES ARRAY IS FOR OBJECT LOOPING
    #### LAST_DAY VAR IS FOR COMPARISON
    print('RUNNING AUDIT')
    print('THIS CAN TAKE A FEW MINUTES')
    workspaces = {}
    LAST_DAY = (datetime.now() - timedelta(hours=24))
    WORKSPACES_AUDIT = []
    DATASOURCES_AUDIT = []
    SCHEMAS_AUDIT = []
    TABLES_AUDIT = []
    COLUMNS_AUDIT = []
    METRICS_AUDIT = []
    MONITORS_AUDIT = []
    ORPHANED_METRICS = []
    USERS_AUDIT = []
    DASHBOARDS_AUDIT = []
    W_USERS_AUDIT = []
    
    #### GET APPLICATION USERS
    print('GETTING APPLICATION USER INFO')
    users_url = f'{SERVER}/api/v0/users'
    get_users_response = json.loads(requests.request("GET", users_url, headers=headers).text)
    for users in get_users_response:
        if users.get('created_at') is not None and datetime.fromtimestamp(users.get('created_at')) >= LAST_DAY:
            USERS_AUDIT.append(["NONE", "APP USER", "CREATION", str(users.get('username')), datetime.fromtimestamp(users.get('created_at')), "NONE"])
    
    #### GET WORKSPACES FOR AUDIT USAGE
    print('GETTING WORKSPACE INFO')
    workspaces_url = f'{BASE_URL}/workspaces'
    get_workspaces_response = json.loads(requests.request("GET", workspaces_url, headers=headers).text)
    
    #### CHECK FOR RECENT WORKSPACE CREATION
    for wksp in get_workspaces_response.get('data'):
        #### ADD WORKSPACE UUID FOR SCHEMA, TABLE, COLUMN, METRIC AUDITING
        workspaces[str(wksp.get('name'))] = str(wksp.get('uuid'))
        if wksp.get('created_at') is not None and datetime.fromtimestamp(wksp.get('created_at')) >= LAST_DAY:
            WORKSPACES_AUDIT.append([str(wksp.get('name')), "WORKSPACE", "CREATION", str(wksp.get('name')), str(wksp.get('created_at')), "NONE"])
            
    for wksp in workspaces:
        #### USERS
        print(f'GETTING USERS INFORMATION FOR {wksp}')
        wksp_users_list_url = f'{SERVER}/api/v0/ws/{workspaces[wksp]}/users'
        get_wksp_users_response = json.loads(requests.request("GET", wksp_users_list_url, headers=headers).text)
        for wksp_user in get_wksp_users_response:
            if wksp_user.get('created_at') is not None and datetime.fromtimestamp(wksp_user.get('created_at')) >= LAST_DAY:
                W_USERS_AUDIT.append([wksp, "WORKSPACE USER", "CREATION", str(wksp_user.get('username')), str(datetime.fromtimestamp(wksp_user.get('created_at'))), f"ROLE - {wksp_user.get('role')}"])
        
        #### DATASOURCES
        print(f'GETTING DATASOURCE INFORMATION FOR {wksp}')
        datasources_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/sources'
        get_datasources_response = json.loads(requests.request("GET", datasources_list_url, headers=headers).text)
        
        for g in get_datasources_response:
            #### GET UNIQUE DATASOURCE INFORMATION
            source_uuid = g.get('metadata', {}).get('uuid')
            source_name = g.get('metadata', {}).get('name')
            
            #### CHECK FOR RECENT DATASOURCE CREATION
            if g.get('status', {}).get('createdTs') is not None and datetime.fromtimestamp(g.get('status', {}).get('createdTs')) >= LAST_DAY:
                DATASOURCES_AUDIT.append([wksp, "DATASOURCE", "CREATION", str(g.get('metadata', {}).get('name')), str(g.get('status', {}).get('createdTs')), str(g.get('metadata', {}).get('ownedBy', {}).get('username'))])
            
            #### CHECK FOR RECENT DATASOURCE UPDATES
            if g.get('status', {}).get('configUpdatedTs') is not None and datetime.fromtimestamp(g.get('status', {}).get('configUpdatedTs')) >= LAST_DAY:
                DATASOURCES_AUDIT.append([wksp, "DATASOURCE", "UPDATE", str(g.get('metadata', {}).get('name')), str(g.get('status', {}).get('configUpdatedTs')), str(g.get('metadata', {}).get('updatedBy', {}).get('username'))])
            
            print(f'GETTING SCHEMA INFORMATION FOR WORKSPACE {wksp} AND DATASOURCE {source_name}')
            #### SCHEMAS
            schemas_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/sources/{source_uuid}/profile/schemas'
            get_schema_response = json.loads(requests.request("GET", schemas_list_url, headers=headers).text)
            
            #### LOOP THROUGH SCHEMAS
            for schema in get_schema_response.get('data'):
                #### CHECK FOR REMOVED SCHEMAS
                if schema.get('removedTs') is not None and datetime.fromtimestamp(schema.get('removedTs')) >= LAST_DAY:
                    SCHEMAS_AUDIT.append([wksp, "SCHEMA", "REMOVED", str(schema.get('name')), str(datetime.fromtimestamp(schema.get('removedTs'))), "NONE"])
                
                #### CHECK FOR NEW SCHEMAS
                if schema.get('firstSeenTs') is not None and datetime.fromtimestamp(schema.get('firstSeenTs')) >= LAST_DAY:
                    SCHEMAS_AUDIT.append([wksp, "SCHEMA", "DISCOVERED", str(schema.get('name')), str(datetime.fromtimestamp(schema.get('firstSeenTs'))), "NONE"])
            
            #### LOOP THROUGH TABLES
            print(f'GETTING TABLE INFORMATION FOR WORKSPACE {wksp} AND DATASOURCE {source_name}')
            tables_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/sources/{source_uuid}/tables'
            get_tables_response = json.loads(requests.request("GET", tables_list_url, headers=headers).text)
            
            #### LOOP THROUGH TABLES
            for g in get_tables_response:
                #### GET TABLE UUID FOR API
                table = str(g.get('tableUuid'))
                table_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/sources/{source_uuid}/profile/tables/{table}'
                get_table_response = json.loads(requests.request("GET", table_list_url, headers=headers).text)
                #### ADD NEW TABLES
                if g.get('firstSeenTs') is not None and datetime.fromtimestamp(g.get('firstSeenTs')) >= LAST_DAY:
                    TABLES_AUDIT.append([wksp, "TABLE", "DISCOVERED", str(g.get('tableName')), str(datetime.fromtimestamp(get_table_response.get('firstSeenTs'))), "NONE"])
            
                #### GET COLUMN API URL
                column_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/sources/{source_uuid}/profile/tables/{table}/columns'
                get_column_response = json.loads(requests.request("GET", column_list_url, headers=headers).text)
                #### LOOP THROUGH COLUMNS
                for c in get_column_response:
                    if c.get('firstSeenTs') is not None and datetime.fromtimestamp(c.get('firstSeenTs')) >= LAST_DAY:
                        COLUMNS_AUDIT.append([wksp, "COLUMN", "DISCOVERED", str(c.get('columnName')), str(datetime.fromtimestamp(c.get('firstSeenTs'))), "NONE"])
            
            #### STORE MONITORS FOR METRIC COMPARISON
            monitors_set = set()
            
            #### CREATE MONITORS SEPARATOR
            print(f'GETTING MONITOR INFORMATION FOR WORKSPACE {wksp} AND DATASOURCE {source_name}')
            monitors_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/monitors'
            get_monitors_response = json.loads(requests.request("GET", monitors_list_url, headers=headers).text)
                
            #### LOOP THROUGH MONITORS
            for monitor in get_monitors_response.get('data'):
                created = int(monitor.get('status', {}).get('createdTs'))
                updated = int(monitor.get('status', {}).get('configUpdatedTs'))
                if monitor.get('status', {}).get('createdTs') is not None and datetime.fromtimestamp(monitor.get('status', {}).get('createdTs')) >= LAST_DAY and created >= updated:
                    MONITORS_AUDIT.append([wksp, "MONITOR", "CREATED", str(monitor.get('metadata', {}).get('name')), str(datetime.fromtimestamp(monitor.get('status', {}).get('createdTs'))), str(monitor.get('metadata', {}).get('ownedBy', {}).get('username'))])
                
                if monitor.get('status', {}).get('configUpdatedTs') is not None and datetime.fromtimestamp(monitor.get('status', {}).get('configUpdatedTs')) >= LAST_DAY and created < updated:
                    MONITORS_AUDIT.append([wksp, "MONITOR", "UPDATED", str(monitor.get('metadata', {}).get('name')), str(datetime.fromtimestamp(monitor.get('status', {}).get('configUpdatedTs'))), str(monitor.get('metadata', {}).get('updatedBy', {}).get('username'))])
                
                monitors_set.add(str(monitor.get('metadata', {}).get('uuid')))
            
            #### LOOP THROUGH METRICS
            print(f'GETTING METRIC INFORMATION FOR WORKSPACE {wksp} AND DATASOURCE {source_name}')
            metrics_list_url = f'{BASE_URL}/ws/{workspaces[wksp]}/metrics'
            get_metrics_response = json.loads(requests.request("GET", metrics_list_url, headers=headers).text)
                            
            for metric in get_metrics_response:
                created = int(metric.get('status', {}).get('createdTs'))
                updated = int(metric.get('status', {}).get('configUpdatedTs'))
                if metric.get('status', {}).get('createdTs') is not None and datetime.fromtimestamp(metric.get('status', {}).get('createdTs')) >= LAST_DAY and created >= updated:
                    MONITORS_AUDIT.append([wksp, "METRIC", "CREATED", str(metric.get('metadata', {}).get('name')), str(datetime.fromtimestamp(metric.get('status', {}).get('createdTs'))), str(metric.get('metadata', {}).get('ownedBy', {}).get('username'))])
                if metric.get('status', {}).get('createdTs') is not None and datetime.fromtimestamp(metric.get('status', {}).get('createdTs')) >= LAST_DAY and created < updated:
                    MONITORS_AUDIT.append([wksp, "METRIC", "UPDATED", str(metric.get('metadata', {}).get('name')), str(datetime.fromtimestamp(metric.get('status', {}).get('configUpdatedTs'))), str(metric.get('metadata', {}).get('updatedBy', {}).get('username'))])
                
                if metric.get('metadata', {}).get('uuid') not in monitors_set:
                    now = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
                    mdt = datetime.fromtimestamp(metric.get('status', {}).get('createdTs'))
                    diff = now - mdt
                    hour_diff = (diff.days * 24) + (diff.seconds // 3600)
                    if hour_diff > 24:
                        ORPHANED_METRICS.append([str(metric.get('metadata', {}).get('name')), str(metric.get('metadata', {}).get('ownedBy', {}).get('username')), str(datetime.fromtimestamp(metric.get('status', {}).get('createdTs')))])
    
    AUDIT_LIST = []
    if len(WORKSPACES_AUDIT) > 0:
        for wa in WORKSPACES_AUDIT:
            AUDIT_LIST.append(wa)
    if len(DATASOURCES_AUDIT) > 0:
        for da in DATASOURCES_AUDIT:
            AUDIT_LIST.append(da)
    if len(SCHEMAS_AUDIT) > 0:
        for sa in SCHEMAS_AUDIT:
            AUDIT_LIST.append(sa)
    if len(TABLES_AUDIT) > 0:
        for ta in TABLES_AUDIT:
            AUDIT_LIST.append(ta)
    if len(COLUMNS_AUDIT) > 0:
        for ca in COLUMNS_AUDIT:
            AUDIT_LIST.append(ca)
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
    
    with xl.Workbook("./audit_output.xlsx") as workbook:
        if len(AUDIT_LIST) > 0:
            fields = ["WORKSPACE", "OBJECT", "EVENT", "OBJECT NAME", "EVENT TIME", "USER NAME"]
            row = 1
            audit_worksheet = workbook.add_worksheet('AUDIT')
            audit_worksheet.write_row(0, 0, fields)
            for al in AUDIT_LIST:
                audit_worksheet.write_row(row, 0, al)
                row += 1
        
        if len(ORPHANED_METRICS) > 0:
            fields = ["METRIC NAME", "CREATED BY", "CREATED AT"]
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
        workspaces[w['name']] = w['uuid']
    
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
        datasources[d['metadata']['name']] = d['metadata']['uuid']

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
    
    datasource = datasources.get(source_choice['ds'])
    
    ### Exit route
    if source_choice['ds'] == 'EXIT':
        exit()
        
    if source_choice['ds'] == 'USERS':
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
    
    for s in schemas['data']:
        if s.get('profilerConfig', {}).get('enabled') == False:
            disabled_schemas[s['name']] = s['uuid']
        else:
            enabled_schemas[s['name']] = s['uuid']
    
    print('''SCHEMAS WITH PROFILING:
''')
    for e in enabled_schemas:
        print(e)
    print('''
SCHEMAS WITHOUT PROFILING:
''')
    for d in disabled_schemas:
        print(d)
    
    next_steps = [
        inquirer.List(
            "next",
            message = "What would you like to do next?",
            choices = ['Schema Details', 'View Tables', 'EXIT'],
        ),
    ]
    
    nxt = inquirer.prompt(next_steps)
    if nxt['next'] == 'EXIT':
        exit()

    if nxt['next'] == 'View Tables':
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
            get_table_info(HEADERS, datasource_url, sch['schema'])
    
    # if nxt['next'] == 'Enable Schemas':
    #     enable_schema = [
    #         inquirer.Checkbox(
    #             "schemas",
    #             message = "What Schema's do you want to enable?",
    #             choices = disabled_schemas.keys(),
    #         ),
    #     ]
    #     es = inquirer.prompt(enable_schema)
    #     for e in es['schemas']:
    #         print(disabled_schemas[e])
    
    if nxt['next'] == 'Schema Details':
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
    
    if gtd['tables'] == 'EXIT':
        exit()
    
    if gtd['tables'] == 'View Tables':
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
            enabled_tables[t['tableName']] = t['tableUuid']
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
    table = enabled_tables.get(table_choice['tb'])
    
    if table_choice['tb'] == 'EXIT':
        exit()
    
    if table_choice['tb'] == 'Unmonitored Tables':
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
    
    print(table_details)

    print(f"""TABLE OVERVIEW
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
    if nxt['next'] == 'EXIT':
        exit()
    if nxt['next'] == 'Columns' :
        get_column_info(HEADERS, table_url)
        
    return table, table_details, table_url

def get_column_info(headers, table_url):
    columns_list_url = f'{table_url}/columns'
    columns = {}

    get_columns_response = json.loads(requests.request("GET", columns_list_url, headers=headers).text)

    for c in get_columns_response:
        columns[c['columnName']] = c['uuid']

    column_list = [
        inquirer.List(
            "col",
            message = "Which Column?",
            choices = columns.keys(),
        ),
    ]
    column_choice = inquirer.prompt(column_list)
    column = columns.get(column_choice['col'])

    column_url = f'{columns_list_url}/{column}'

    get_column_info = json.loads(requests.request("GET", column_url, headers=headers).text)

    print('Column Name: ' + get_column_info['columnName'])
    print('Data Type: ' + get_column_info['columnType'])
    print('Categorical Distribution Enabled: ' + str(get_column_info['profilerConfig']['categoricalDistribution']['enabled']))
    print('Category Tracking Enabled: ' + str(get_column_info['profilerConfig']['categoryListChange']['enabled']))
    print('Numerical Distribution Enabled: ' + str(get_column_info['profilerConfig']['numericalDistribution']['enabled']))
    print('Null% Enabled: ' + str(get_column_info['profilerConfig']['missingValue']['enabled']))


if __name__ == '__main__':
    initial_action = [
        inquirer.List(
            "init",
            message = "Audit or Explore?",
            choices = ['Audit', 'Explore', 'EXIT'],
        )
    ]
    
    init_choice = inquirer.prompt(initial_action)
    if init_choice['init'] == 'EXIT':
        exit()
    elif init_choice['init'] == 'Audit':
        daily_audit(HEADERS)
    else:
        get_workspace_info(HEADERS)
