from airflow.utils.task_group import TaskGroup
from airflow.providers.ssh.operators.ssh import SSHOperator

  
def create_task_group(dag): 
  with TaskGroup(group_id='casts_labels', dag=dag) as dag:
    labels = SSHOperator(
      task_id='labels',
      ssh_conn_id='ssh_worker',
      command='/home/na/worker.sh labels3 run "{{ execution_date.strftime("%Y-%m-%d-%H") }}"',
      cmd_timeout=120,
      get_pty=True)
    labels
  return dag