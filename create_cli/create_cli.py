##########################################################################################
# NAME: Lightup Metric Create
# VERSION: 0.1.0
# DESCRIPTION:
# This Lightup CLI is intended for creating and enabling metrics within Lightup
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

import requests, json, pandas as pd, math

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
    "Accept": "application/json",
    "content-type": "application/json"
}

def get_workspace_uuid_from_workspace_name(w_name):
    try:
        print(f"Getting ID for {w_name}")
        url = f'{BASE_URL}/workspaces'
        workspaces = json.loads(requests.get(url, headers=HEADERS).text)
        for wksp in workspaces["data"]:
            if wksp["name"].upper() == w_name.upper():
                return wksp["uuid"]
    except:
        print(f"Unable to find workspace {w_name}")

def get_source_uuid_from_source_name(s_name, w_id):
    try:
        print(f"Getting ID for {s_name}")
        url = f'{BASE_URL}/ws/{w_id}/sources'
        sources = json.loads(requests.get(url, headers=HEADERS).text)
        for src in sources:
            if src["metadata"]["name"].upper() == s_name.upper():
                return src["metadata"]["uuid"]
    except:
        print(f"Unable to find datasource {s_name} in workspace {w_id}")

def get_table_uuid_from_table_name(t_name, s_id, w_id):
    try:
        print(f"Getting ID for {t_name}")
        url = f'{BASE_URL}/ws/{w_id}/sources/{s_id}/tables'
        tables = json.loads(requests.get(url, headers=HEADERS).text)
        for table in tables:
            if table["tableName"].upper() == t_name.upper():
                return table["tableUuid"]
    except:
        print(f"Unable to find table {t_name} in datasource {s_id} in workspace {w_id}")

def get_column_info_from_column_name(c_name, t_id, s_id, w_id):
    try:
        print(f"Getting info for {c_name}")
        url = f'{BASE_URL}/ws/{w_id}/sources/{s_id}/profile/tables/{t_id}/columns'
        columns = json.loads(requests.get(url, headers=HEADERS).text)
        for col in columns:
            if col["columnName"].upper() == c_name.upper():
                return col["uuid"], col["columnTypeCategory"], col["profilerConfig"], c_name
    except:
        print(f"Unable to find column {c_name} in table {t_id} in datasource {s_id} in workspace {w_id}")

def get_metric_uuid_from_column_id(w_id, c_id, m_type):
    try:
        url = f'{BASE_URL}/ws/{w_id}/metrics/?column_uuids={c_id}'
        metrics = json.loads(requests.get(url, headers=HEADERS).text)
        for m in metrics:
            if m['metadata']['name'] == m_type:
                return m['metadata']['uuid']
    except:
        print(f'Unable to get metric uuid for {c_id}')

def create_threshold_on_null_metric(ub, lb, w_id, m_id, c_name):
    url = f'{BASE_URL}/ws/{w_id}/monitors/'
    m_name = f'{c_name}_threshold'
    payload = {
        "config": {
            "alertConfig": {"isMuted": True},
            "isLive": True,
            "metrics": [m_id],
            "symptom": {
                "bound": {
                },
                "featureConfig": {"type": "value"},
                "type": "manualThreshold"
            }
        },
        "metadata": {
            "name": m_name,
            "workspaceId": w_id
        },
        "type": "rule",
        "apiVersion": "v1"
    }
    if math.isnan(ub) and math.isnan(lb):
        return "No Threshold Provided"
    else:
        if math.isnan(ub) == False:
            if isinstance(ub, int) or isinstance(ub, float):
                payload["config"]["symptom"]["bound"]["upper"] = ub
                
        if math.isnan(lb) == False:
            if isinstance(lb, int) or isinstance(lb, float):
                payload["config"]["symptom"]["bound"]["lower"] = lb
    try:
        response = json.loads(requests.request("POST", url, json=payload, headers=HEADERS).text)
        return response, payload
    except:
        print(f'Unable to create threshold on metric {m_id}')
        return payload

def enable_autometric_on_column(m_type, c_name, c_config, c_type, c_id, t_id, s_id, w_id, ub, lb):
    try:
        url = f'{BASE_URL}/ws/{w_id}/sources/{s_id}/profile/tables/{t_id}/columns/{c_id}/profiler-config'
        payload = c_config
        payload["enabled"] = True
        if m_type == "NULL" or math.isnan(m_type):
            payload["missingValue"]["enabled"] = True
            m_id = get_metric_uuid_from_column_id(w_id, c_id, 'Null Percent')
            thresh_resp = create_threshold_on_null_metric(ub, lb, w_id, m_id, c_name)
        elif m_type.lower() == 'distribution':
            print(f"enabling distribution check on {c_id}")
            if c_type == 'numeric':
                payload["numericalDistribution"]["enabled"] = True
            else:
                payload["categoricalDistribution"]["enabled"] = True
                payload["categoryListChange"]["enabled"] = True
            print(f'dist update: {payload}')
            m_id = get_metric_uuid_from_column_id(w_id, c_id, 'Distribution')
        enable_resp = json.loads(requests.put(url, headers=HEADERS, json=payload).text)
        return enable_resp, thresh_resp
    except:
        print(f'Unable to enable autometric on column {c_name}')


if __name__ == '__main__':
    xls = ''
    create_list = ''
    try:
        xls = "./create_metric.xlsx"
        create_list = pd.read_excel(xls, sheet_name="METRICS")
    except:
        print(f"Unable to find the template file at location {xls}")
    
    for i, r in create_list.iterrows():
        try:
            w_id = get_workspace_uuid_from_workspace_name(r["WORKSPACE"])
            s_id = get_source_uuid_from_source_name(r["DATASOURCE"], w_id)
            t_id = get_table_uuid_from_table_name(r["TABLE"], s_id, w_id)
            c_info = get_column_info_from_column_name(r["COLUMN"], t_id, s_id, w_id)
            try:
                enable_autometric_on_column(r["METRIC"], r["COLUMN"], c_info[2], c_info[1], c_info[0], t_id, s_id, w_id, r["UPPER"], r["LOWER"])
            except:
                print(f'Unable to enable {r["METRIC"]} on column {r["COLUMN"]}')
        except:
            print(f'Unable to enable {r["METRIC"]} on column {r["COLUMN"]}')
