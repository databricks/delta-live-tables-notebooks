from email.policy import default
import time
import argparse
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider, \
    ProfileConfigProvider
from databricks_cli.sdk import DeltaPipelinesService

def create_api_client(profile=''):
    try:
        if profile == '':
            config = EnvironmentVariableConfigProvider().get_config()
        else:
            if profile == 'default':
                config = ProfileConfigProvider().get_config()
            else:
                config = ProfileConfigProvider(profile).get_config()
        
        api_client = _get_api_client(config, command_name="blog-dms-cdc-demo")
    except:
        print("Failed to create api client.") 
        print("Please create DATABRICKS_HOST and DATABRICKS_TOKEN env variables")
        print("Or pass Databrick profile as such --profile")

    return api_client


def update_and_monitor(api_client, pipeline_id, full_refresh=False):
    pipeline_service = DeltaPipelinesService(api_client)

    pipeline_update_id = pipeline_service.start_update(
        pipeline_id=pipeline_id,
        full_refresh=full_refresh).get('update_id', '')

    if pipeline_update_id == '':
        raise Exception("Updating pipeline failed")

    time.sleep(2) # quick pause so that we can get the update state
    print("Started Pipeline Update.")
    while True:
        print("Pipeline still updating...")
        pipeline_info = pipeline_service.get(pipeline_id=pipeline_id)
        pipeline_updates = pipeline_info['latest_updates']
        
        pipeline_update_state = list(filter(lambda updates: updates['update_id'] ==\
            pipeline_update_id, pipeline_updates))[0]['state']

        if pipeline_update_state in ['FAILED','CANCELED']:
            print(f'Pipeline Update Ended with the {pipeline_update_state}')
            break
        elif pipeline_update_state in ['COMPLETED']:
            print('Pipeline Completed Successfully.')
            break
    
        time.sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get Job Info')
    parser.add_argument('--pipeline-id', type=str, required=True)
    parser.add_argument('--profile', type=str, default='')
    parser.add_argument('--full-refresh', type=bool, default=False)
    args = parser.parse_args()

    api_client = create_api_client(args.profile)
    update_and_monitor(api_client, args.pipeline_id, full_refresh=args.full_refresh)