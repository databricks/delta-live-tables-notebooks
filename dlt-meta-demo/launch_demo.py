"""Inegration tests script."""
import os
import time
import uuid
import glob
import subprocess
import argparse
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService, DbfsService, DeltaPipelinesService, WorkspaceService
import base64
import json


def get_api_client():
    """Get api client with config."""
    config = EnvironmentVariableConfigProvider().get_config()
    api_client = _get_api_client(config, command_name="labs_dlt-meta")
    return api_client


cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


def create_workflow_spec(job_spec_dict):
    """Create Job specification."""
    job_spec = {
        "run_name": f"dais-dlt-meta-demo-{job_spec_dict['run_id']}",
        "tasks": [
            {
                "task_key": "setup_dlt_meta_pipeline_spec",
                "description": "Sets up metadata tables for DLT-META",
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "python_wheel_task": {
                    "package_name": "dlt_meta",
                    "entry_point": "run",
                    "named_parameters": {
                        "onboard_layer": "bronze_silver",
                        "database": job_spec_dict['database'],
                        "onboarding_file_path": f"{job_spec_dict['dbfs_tmp_path']}/conf/onboarding.json",
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": f"{job_spec_dict['dbfs_tmp_path']}/data/dlt_spec/silver",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{job_spec_dict['dbfs_tmp_path']}/data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": job_spec_dict['env']
                    },
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "dlt-meta"
                        }
                    }
                ]
            },
            {
                "task_key": "bronze_initial_run",
                "depends_on": [
                    {
                        "task_key": "setup_dlt_meta_pipeline_spec"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['bronze_pipeline_id']
                }
            },
            {
                "task_key": "silver_initial_run",
                "depends_on": [
                    {
                        "task_key": "bronze_initial_run"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['silver_pipeline_id']
                }
            },
            {
                "task_key": "load_incremental_data",
                "description": "Load Incremental Data",
                "depends_on": [
                    {
                        "task_key": "silver_initial_run"
                    }
                ],
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "notebook_task": {
                    "notebook_path": f"{job_spec_dict['runners_nb_path']}/runners/load_incremental_data",
                    "base_parameters": {
                        "dbfs_tmp_path": job_spec_dict['dbfs_tmp_path']
                    }
                }

            },
            {
                "task_key": "bronze_incremental_run",
                "depends_on": [
                    {
                        "task_key": "load_incremental_data"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['bronze_pipeline_id']
                }
            },
            {
                "task_key": "silver_incremental_run",
                "depends_on": [
                    {
                        "task_key": "bronze_incremental_run"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['silver_pipeline_id']
                }
            },
        ]
    }
    print(job_spec)
    return job_spec


def create_dlt_meta_pipeline(
        pipeline_service: DeltaPipelinesService,
        runners_nb_path,
        run_id, configuration={}):
    """Create DLT pipeline."""
    return pipeline_service.create(
        name=f"dais-dlt-meta-{configuration['layer']}-{run_id}",
        clusters=[
            {
                "label": "default",
                "num_workers": 1
            }
        ],
        configuration=configuration,
        libraries=[
            {
                "notebook": {
                    "path": f"{runners_nb_path}/runners/init_dlt_meta_pipeline"
                }
            }
        ],
        target=f"{configuration['layer']}_{run_id}"
    )['pipeline_id']


class JobSubmitRunner():
    """Job Runner class."""

    def __init__(self, job_client: JobsService, job_dict):
        """Init method."""
        self.job_dict = job_dict
        self.job_client = job_client

    def submit(self):
        """Submit job."""
        return self.job_client.submit_run(**self.job_dict)

    def monitor(self, run_id):
        """Monitor job using runId."""
        while True:
            self.run_res = self.job_client.get_run(run_id)
            self.run_url = self.run_res["run_page_url"]
            self.run_life_cycle_state = self.run_res['state']['life_cycle_state']
            self.run_result_state = self.run_res['state'].get('result_state')
            self.run_state_message = self.run_res['state'].get('state_message')

            if self.run_life_cycle_state in ['PENDING', 'RUNNING', 'TERMINATING']:
                print("Job still running current life Cycle State is " + self.run_life_cycle_state)
            elif self.run_life_cycle_state in ['TERMINATED']:
                print("Job terminated")

                if self.run_result_state in ['SUCCESS']:
                    print("Job Succeeded")
                    print(f"Run URL {self.run_url}")
                    break
                else:
                    print("Job failed with the state of " + self.run_result_state)
                    print(self.run_state_message)
                    break
            else:
                print(
                    "Job was either Skipped or had Internal error please check the job ui and run in Databricks to see why")
                print(self.run_state_message)
                break

            time.sleep(20)


def create_onboarding(dbfs_tmp_path, run_id):
    
    """Create onboarding file for cloudfiles as source."""
    onboarding_template = "demo/conf/onboarding.template"
    with open(f"{onboarding_template}") as f:
        onboard_obj = json.load(f)

    for data_flow in onboard_obj:
        for key, value in data_flow.items():
            if key == "source_details":
                for source_key, source_value in value.items():
                    if 'dbfs_path' in source_value:
                        data_flow[key][source_key] = source_value.format(dbfs_path=dbfs_tmp_path)
            if 'dbfs_path' in value:
                data_flow[key] = value.format(dbfs_path=dbfs_tmp_path)
            elif 'run_id' in value:
                data_flow[key] = value.format(run_id=run_id)

    with open("demo/conf/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


def main():
    """Entry method to run integration tests."""
    args = process_arguments()

    api_client = get_api_client()
    username = api_client.perform_query("GET", "/preview/scim/v2/Me").get("userName")
    run_id = uuid.uuid4().hex
    dbfs_tmp_path = f"{args.__dict__['dbfs_path']}/{run_id}"
    database = f"dais_dlt_meta_{run_id}"
    int_tests = "demo/"
    runners_nb_path = f"/Users/{username}/dais_dlt_meta/{run_id}"
    runners_full_local_path = 'demo/dais_dlt_meta_runners.dbc'

    dbfs_service = DbfsService(api_client)
    jobs_service = JobsService(api_client)
    workspace_service = WorkspaceService(api_client)
    pipeline_service = DeltaPipelinesService(api_client)

    try:
        create_onboarding(dbfs_tmp_path, run_id)

        DbfsApi(api_client).cp(True, True, int_tests, dbfs_tmp_path + "/")
        fp = open(runners_full_local_path, "rb")
        workspace_service.mkdirs(path=runners_nb_path)
        workspace_service.import_workspace(path=f"{runners_nb_path}/runners", format="DBC",
                                           content=base64.encodebytes(fp.read()).decode('utf-8'))
        bronze_pipeline_id = create_dlt_meta_pipeline(
            pipeline_service, runners_nb_path, run_id, configuration={
                "layer": "bronze",
                "bronze.group": "A1",
                "bronze.dataflowspecTable": f"{database}.bronze_dataflowspec_cdc"
            }
        )

        cloud_node_type_id_dict = {"aws": "i3.xlarge",
                                   "azure": "Standard_D3_v2",
                                   "gcp": "n1-highmem-4"
                                   }
        job_spec_dict = {"run_id": run_id,
                         "dbfs_tmp_path": dbfs_tmp_path,
                         "runners_nb_path": runners_nb_path,
                         "database": database,
                         "env": "prod",
                         "bronze_pipeline_id": bronze_pipeline_id,
                         "node_type_id": cloud_node_type_id_dict[args.__dict__['cloud_provider_name']],
                         "dbr_version": args.__dict__['dbr_version']
                         }

        silver_pipeline_id = create_dlt_meta_pipeline(
            pipeline_service, runners_nb_path, run_id, configuration={
                "layer": "silver",
                "silver.group": "A1",
                "silver.dataflowspecTable": f"{database}.silver_dataflowspec_cdc"
            }
        )
        job_spec_dict["silver_pipeline_id"] = silver_pipeline_id

        job_spec = create_workflow_spec(job_spec_dict)
        
        job_submit_runner = JobSubmitRunner(jobs_service, job_spec)

        job_run_info = job_submit_runner.submit()
        print(f"Run URL {job_run_info['run_id']}")

        job_submit_runner.monitor(job_run_info['run_id'])

    except Exception as e:
        print(e)
    finally:
        pipeline_service.delete(bronze_pipeline_id)
        pipeline_service.delete(silver_pipeline_id)
        dbfs_service.delete(dbfs_tmp_path, True)
        workspace_service.delete(runners_nb_path, True)
        try:
            os.remove("conf/onboarding.json")
        except Exception as e:
            print(e)


def process_arguments():
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloud_provider_name",
                        help="provide cloud provider name. Supported values are aws , azure , gcp")
    parser.add_argument("--dbr_version", help="Provide databricks runtime spark version e.g 11.3.x-scala2.12")
    parser.add_argument("--dbfs_path",
                        help="Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/")
    args = parser.parse_args()
    mandatory_args = ["cloud_provider_name", "dbr_version", "dbfs_path"]
    check_mandatory_arg(args, mandatory_args)

    supported_cloud_providers = ["aws", "azure", "gcp"]

    cloud_provider_name = args.__getattribute__("cloud_provider_name")
    if cloud_provider_name.lower() not in supported_cloud_providers:
        raise Exception("Invalid value for --cloud_provider_name! Supported values are aws, azure, gcp")

    print(f"Parsing argument complete. args={args}")
    return args


def check_mandatory_arg(args, mandatory_args):
    """Check mandatory argument present."""
    for mand_arg in mandatory_args:
        if args.__dict__[f'{mand_arg}'] is None:
            raise Exception(f"Please provide '--{mand_arg}'")


if __name__ == "__main__":
    main()
