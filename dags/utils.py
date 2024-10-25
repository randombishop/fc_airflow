import datetime
import logging
import os
import time
import tempfile
from google.cloud import notebooks_v1
from google import auth
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def exec_notebook(
  task,
  description,
  gcs_notebook,
  instance_type,
  container_image_uri,
  kernel_spec,
  params):
  logging.info("vertex_exec_notebook")
  region = os.environ['EXECUTOR_REGION']
  network = os.environ['EXECUTOR_NETWORK']
  executor_folder = os.environ['EXECUTOR_FOLDER']
  service_account = os.environ['EXECUTOR_SERVICE_ACCOUNT']
  _, project = auth.default()
  parent = "projects/" + project + "/locations/" + region
  execution_id = task+'_'+str(datetime.datetime.now()).replace(' ','').replace('-','').replace(':','')[:12]
  output_notebook = executor_folder + '/outputs/' + execution_id + '.ipynb'
  params_s = ','.join([k+'='+v for k,v in params.items()])
  execution_config = {
    "execution_template": {
      "scale_tier": "CUSTOM",
      "master_type": instance_type,
      "input_notebook_file": gcs_notebook,
      "output_notebook_folder": executor_folder,
      "container_image_uri": container_image_uri,
      "parameters": params_s,
      "service_account": service_account,
      "kernel_spec": kernel_spec,
      "vertex_ai_parameters": {
          "network": network
      }
    },
    "display_name": task,
    "description": description,
    "output_notebook_file": output_notebook
  }
  logging.info('execution_config='+str(execution_config))
  client = notebooks_v1.NotebookServiceClient()
  logging.info("notebooks_v1 client="+str(client))
  request = notebooks_v1.CreateExecutionRequest(
    parent=parent,
    execution_id=execution_id,
    execution=execution_config
  )
  execution = client.create_execution(request=request)
  response = execution.result()
  execution_name = response.name
  logging.info('execution_name='+execution_name)
  response = client.get_execution(name=execution_name)
  check_every_sec = 120
  while response.state in [notebooks_v1.Execution.State.QUEUED, notebooks_v1.Execution.State.PREPARING, notebooks_v1.Execution.State.RUNNING]:
    logging.info('current state='+str(response.state))
    time.sleep(check_every_sec)
    response = client.get_execution(name=execution_name)
  logging.info('final state='+str(response.state))
  if response.state != notebooks_v1.Execution.State.SUCCEEDED:
    raise Exception(f"Notebook execution failed")
    

def exec_dune_query(query_id, params):
  query = QueryBase(query_id=query_id,params=params)
  dune = DuneClient.from_env()
  df = dune.run_query_dataframe(query)
  df.replace('<nil>', None, inplace=True)
  return df


def dataframe_to_gcs(df, destination, bucket_name = 'dsart_nearline1'):
  with tempfile.NamedTemporaryFile(mode='w+', suffix=".csv", delete=True) as temp_file:
    temp_csv_path = temp_file.name
    df.to_csv(temp_csv_path, index=False)
    logging.info(f"DataFrame saved to temp CSV file at {temp_csv_path}")
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') 
    gcs_hook.upload(
      bucket_name=bucket_name,
      object_name=destination,
      filename=temp_csv_path
    )
    print(f"File {temp_csv_path} uploaded to GCS at {destination}")
