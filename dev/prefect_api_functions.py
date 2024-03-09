"""This module connects to prefect api to get operational data about image downlaoding process."""
from prefect import task
from prefect.context import FlowRunContext
import requests
@task()
def get_flow_id():
    flow_run_id = FlowRunContext.get().flow_run.dict().get('id')
    print(f'DM : flow_run_id from inside Auditer is :  {flow_run_id}')
    print (f'DM : type of flow_run_id is : {type(flow_run_id)}')
    return flow_run_id

def Get_audit_data(flow_run_id,saved_Images , saved_metadata):
    Audit_rows = []
    flow_state = requests.get(f'http://127.0.0.1:4200/api/flow_runs/{flow_run_id}').json()
    # flow_state.raise_for_status()
    print(flow_state)
    flow_id = flow_state['flow_id']
    flow_run_id = flow_state['id']
    start_time = flow_state['start_time']
    end_time = flow_state['end_time']
    status = flow_state['state_type']
    WebsiteUser = 'Bahnsasawy'
    Client = 'Ali'
    Contract_id = '66'
    deployment_id = flow_state['deployment_id']
    for i in range(len(saved_Images)):
        Audit_rows.append([saved_Images[i] , saved_metadata[i]   , WebsiteUser , Client,Contract_id , deployment_id ,flow_id, flow_run_id , start_time , end_time , status])
    
    return Audit_rows

