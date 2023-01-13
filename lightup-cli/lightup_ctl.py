import requests, json, inquirer, datetime

################## GET ACCESS TOKEN ##################
BASE_URL = "https://<INSERT BASE URL>/api/v1"

# SEE THIS DOC FOR MORE INFO: https://docs.lightup.ai/reference/post_api-v1-token-refresh
REFRESH_TOKEN = "<INSERT LIGHTUP API TOKEN>"

TOKEN_PAYLOAD = {"refresh": REFRESH_TOKEN}
TOKEN_URL = f'{BASE_URL}/token/refresh/'
TOKEN_RESPONSE = json.loads(requests.request("POST", TOKEN_URL, json=TOKEN_PAYLOAD).text)

ACCESS_TOKEN = 'Bearer ' + str(TOKEN_RESPONSE.get('access'))

HEADERS = {
    "Authorization": ACCESS_TOKEN,
    "Accept": "application/json"
}


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
    workspaces['EXIT'] = ['EXIT']

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
    else:
        ### Print Workspace details
        workspace_url = f'{BASE_URL}/workspaces/{workspace}'
        
        for d in json.loads(requests.request("GET", workspaces_url, headers=headers).text).get('data'):
            if workspace_choice.get('wksp') == d.get('name'):
                print(f"""
Workspace Name: {str(d.get('name'))}
Workspace Description: {str(d.get('description'))}
Created at: {str(datetime.datetime.fromtimestamp(d.get('created_at')))}
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
    datasources['EXIT'] = ['EXIT']
    
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

    ### Datasource URL to pass to the Get Details function
    datasource_url = f'{datasources_list_url}/{datasource}'
    
    ### Get datasource details
    datasource_details = json.loads(requests.request("GET", datasource_url, headers=headers).text)
    ds_type = datasource_details.get('config', {}).get('connection', {}).get('type')
    
    ### Print generic datasource details
    print(f"""
Datasource Name: {str(datasource_details.get('metadata', {}).get('name'))}
Datasource Type: {str(datasource_details.get('config', {}).get('connection', {}).get('type'))}
Owner: {str(datasource_details.get('metadata', {}).get('ownedBy', {}).get('email', datasource_details.get('metadata', {}).get('updatedBy', {}).get('email')))}
Created At: {str(datetime.datetime.fromtimestamp(datasource_details.get('status', {}).get('createdTs')))}
Profiling Enabled: {str(datasource_details.get('config', {}).get('isLive'))}
Datasource Tags: {str(datasource_details.get('metadata', {}).get('tags'))}""")
    if str(datasource_details.get('status', {}).get('lastScannedStatus')) == 'success':
        print("Successful Last Scan: " + str(datetime.datetime.fromtimestamp(datasource_details.get('status', {}).get('lastScannedTs'))))
    else:
        print(f"""
Last Scan Failed At: {str(datetime.datetime.fromtimestamp(datasource_details.get('status', {}).get('lastScannedTs')))}
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
            fs = str(datetime.datetime.fromtimestamp(schema_details.get('firstSeenTs'))) if schema_details.get('firstSeenTs') != None else "Never Seen"
            lse = str(datetime.datetime.fromtimestamp(schema_details.get('lastSeenTs'))) if schema_details.get('lastSeenTs') != None else "Never Seen"
            rem = str(datetime.datetime.fromtimestamp(schema_details.get('removedTs'))) if schema_details.get('removedTs') != None else "Still Live"
            lts = str(datetime.datetime.fromtimestamp(schema_details.get('lastTablesScannedTs'))) if schema_details.get('lastTablesScannedTs') != None else "Never Scanned"
            lsc = str(datetime.datetime.fromtimestamp(schema_details.get('lastScannedTs'))) if schema_details.get('lastScannedTs') != None else "Never Scanned"
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
Last Seen: {str(datetime.datetime.fromtimestamp(table_details.get('lastSeenTs')))}
Last Scanned: {str(datetime.datetime.fromtimestamp(table_details.get('lastScannedTs')))}
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
Data Delay Last Check: {str(datetime.datetime.fromtimestamp(table_details.get('status', {}).get('dataDelay', {}).get('lastEventTs')))}
Data Volume Last Check: {str(datetime.datetime.fromtimestamp(table_details.get('status', {}).get('tableVolume', {}).get('lastEventTs')))}
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
    get_workspace_info(HEADERS)
