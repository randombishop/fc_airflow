import datetime
import logging
import os
import time
import tempfile
import io
import requests
from google.cloud import notebooks_v1
from google import auth
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.api.table import TableAPI
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
    logging.info(f"File {temp_csv_path} uploaded to GCS at {destination}")


def dataframe_to_dune(df, namespace, table_name):
  logging.info("dataframe_to_dune")
  dune = TableAPI(api_key=os.environ['DUNE_API_KEY'])
  buffer = io.BytesIO()
  df.to_csv(buffer, index=False)
  content_type = 'text/csv'
  #df.to_json(buffer, orient="records", lines=True)
  #content_type = 'application/x-ndjson'
  response = dune.insert_table(
    namespace=namespace,
    table_name=table_name,
    data=buffer.getvalue(),
    content_type=content_type
  )
  logging.info(f"Dune response: {response}")


def pull_trending_casts():
  url = "https://api.neynar.com/v2/farcaster/feed/trending?limit=10&time_window=1h&provider=neynar"
  headers = {
      "accept": "application/json",
      "x-api-key": os.environ['NEYNAR_API_KEY']
  }
  response = requests.get(url, headers=headers).json()
  casts = [parse_cast(data) for data in response['casts']]
  return casts


def parse_cast(data):
  cast = {}
  cast['timestamp'] = data['timestamp']
  cast['hash'] = data['hash']
  cast['fid'] = data['author']['fid']
  cast['username'] = data['author']['username']
  cast['text'] = data['text']
  cast['parent_hash'] = data['parent_hash']
  cast['parent_url'] = data['parent_url']
  cast['root_parent_url'] = data['root_parent_url']
  has_address = 'profile' in data['author'] and 'location' in data['author']['profile'] and 'address' in data['author']['profile']['location']
  address = data['author']['profile']['location']['address'] if has_address else None
  if address is not None:
    cast['profile_country'] = address['country_code']
  cast['follower_count'] = data['author']['follower_count']
  cast['following_count'] = data['author']['following_count']
  if 'embeds' in data and len(data['embeds']) > 0:
    first_embed = data['embeds'][0]
    if 'url' in first_embed:
      cast['embed_url'] = first_embed['url']
    elif 'cast' in first_embed:
      cast['embed_hash'] = first_embed['cast']['hash']
      cast['embed_fid'] = first_embed['cast']['author']['fid']
      cast['embed_username'] = first_embed['cast']['author']['username']
      cast['embed_text'] = first_embed['cast']['text']
  if 'reactions' in data:
    cast['likes_count'] = data['reactions']['likes_count']
    cast['recasts_count'] = data['reactions']['recasts_count']
  if 'replies' in data:
    cast['replies_count'] = data['replies']['count']
  return cast
