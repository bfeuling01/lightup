import requests, json, inquirer

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

def get_wksp(headers):
    workspaces = {}
    workspaces_url = f'{BASE_URL}/workspaces'
    get_workspaces_response = json.loads(requests.request("GET", workspaces_url, headers=headers).text).get('data')

    for w in get_workspaces_response:
        workspaces[w.get('name')] = w.get('uuid')

    workspaces['EXIT'] = 'EXIT'

    workspace_list = [
            inquirer.List(
                "wksp",
                message = "Which Workspace?",
                choices = workspaces.keys(),
            ),
        ]

    workspace_choice = inquirer.prompt(workspace_list)
    workspace = workspaces.get(workspace_choice.get('wksp'))
        
    if workspace_choice.get('wksp') == 'EXIT':
        exit()
    else:
        workspace_url = f'{BASE_URL}/ws/{workspace}'
        get_metrics(HEADERS, workspace_url)
        
    
def get_metrics(headers, url):
    metrics = {}
    tables = {}
    get_metrics_response = json.loads(requests.request("GET", f'{url}/metrics/', headers=headers).text)
    for g in get_metrics_response:
        if g.get('config', {}).get('triggered') == True:
            source_id = g.get('config', {}).get('sources')[0]
            table_name = ''
            table_id = ''
            if g.get('config', {}).get('table') is not None:
                table_name = g.get('config', {}).get('table', {}).get('tableName')
                table_id = g.get('config', {}).get('table', {}).get('tableUuid')
            else:
                table_name = g.get('config', {}).get('targetTable', {}).get('table', {}).get('tableName')
                table_id = g.get('config', {}).get('targetTable', {}).get('table', {}).get('tableUuid')
            tables[table_name] = [table_id, source_id]
            metrics[g.get('metadata', {}).get('name')] = [g.get('metadata', {}).get('workspaceId'), source_id, g.get('metadata', {}).get('uuid')]
            
    triggers = {}
    triggers['EXIT'] = 'EXIT'
    
    trigger_intent = {}
    trigger_intent['METRICS'] = 'METRICS'
    trigger_intent['TABLES'] = 'TABLES'
    trigger_intent['EXIT'] = 'EXIT'

    trigger_list = [
            inquirer.List(
                "trig",
                message = "Individual Metrics or Table Wide Metrics",
                choices = trigger_intent.keys(),
            ),
        ]

    trigger_choice = inquirer.prompt(trigger_list)
        
    if trigger_choice.get('trig') == 'EXIT':
        exit()
    elif trigger_choice.get('trig') == 'TABLES':
        table_choice = [
            inquirer.Checkbox(
                "tables",
                message = "Which tables should be triggered?",
                choices = tables.keys(),
            )
        ]
        tables_chosen = inquirer.prompt(table_choice)
        for t in tables_chosen['tables']:
            trigger_url = f'{url}/sources/{tables[t][1]}/trigger'
            table_payload = {"table_uuids": [tables[t][0] for t in tables_chosen['tables']]}
            table_triggering = json.loads(requests.post(trigger_url, json=table_payload, headers=headers).text)
            print(table_triggering)
    else:
        metric_choice = [
            inquirer.Checkbox(
                "metrics",
                message = "Which metrics should be triggered?",
                choices = metrics.keys(),
            )
        ]
        metrics_chosen = inquirer.prompt(metric_choice)
        for m in metrics_chosen['metrics']:
            trigger_url = f'{url}/sources/{metrics[m][1]}/trigger'
            metric_payload = {"metric_uuids": [metrics[m][2] for m in metrics_chosen['metrics']]}
            metric_triggering = json.loads(requests.post(trigger_url, json=metric_payload, headers=headers).text)
            print(metric_triggering)


if __name__ == '__main__':
    get_wksp(HEADERS)
