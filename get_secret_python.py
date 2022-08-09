import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


import requests
import json

# Import the Secret Manager client library.
from google.cloud import secretmanager

def get_password(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    


    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    # response = client.access_secret_version(request={"name": name})
    response = client.access_secret_version(name=name)

    # Verify payload checksum.
    '''
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)
    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        print("Data corruption detected.")
        return response
    '''
    # Getting the secret payload.
    payload = response.payload.data.decode("UTF-8")
    return payload
    # return format(payload)
    
    # print("Plaintext: {}".format(payload))











def open_session(ti):
    # LOGIN TO INFA

    # is used to get icSessionId
    url = 'https://dm-em.informaticacloud.com/ma/api/v2/user/login'
    
    project_id = "tenacious-post-355715"
    secret_id = "secret-infa"
    version_id = "1"
    
    password = get_password(project_id, secret_id, version_id)
    
    myobj = {
        "@type":"login",
        "username":"ssaprykin",
        "password":password
    }

    # x is response from INFA
    x = requests.post(url, json = myobj)
    # make response as json to be able to read as dictionary
    json_obj = x.json()

    # informatica session id
    session_id = json_obj["icSessionId"]
    print(session_id)
    
    # session_id below is INFA session id
    ti.xcom_push(key='session_id', value=session_id)
    
    # return session_id


def run_a_task(ti):
    # START A TASK

    url_job = 'https://emw1.dm-em.informaticacloud.com/saas/api/v2/job'
    
    # get session_id from another function
    
    # accuracies = ti.xcom_pull(key='model_accuracy', session_id=['training_model_A', 'training_model_B', 'training_model_C'])
    session_id = ti.xcom_pull(key='session_id', task_ids='open_session')
        
    Headers = {"icSessionId": session_id}

    myobj = {
        "@type":"job",
        "taskId":"0119EH0I000000000002",
        "taskType":"DSS"
    }

    y = requests.post(url_job, headers=Headers, json = myobj)
    print(y)



def close_session():
    # CLOSE SESSION

    url_logout = "https://dm-em.informaticacloud.com/ma/api/v2/user/logout"

    myobj = {
        "@type":"logout"
    }
    z = requests.post(url_logout, json = myobj)
    print(z)
    print('dag is split in functions')
    return "session is closed"



default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }


dag_python = DAG(
	dag_id = "infa-secret-AF",
	default_args=default_args,
	# schedule_interval='0 0 * * *',
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of python operator in airflow',
	start_date = airflow.utils.dates.days_ago(1))


# let's try this
open_session = PythonOperator(
    task_id='open_session', 
    python_callable=open_session, 
    dag=dag_python
    )

run_a_task = PythonOperator(
    task_id='run_a_task', 
    python_callable=run_a_task,
    dag=dag_python
    )

close_session = PythonOperator(
    task_id='log_out', 
    python_callable=close_session, 
    dag=dag_python
    )


# let's try this
# open_session = open_session()
open_session >> run_a_task >> close_session

# two links to pass params
# https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html
# https://stackoverflow.com/questions/54894418/how-to-pass-parameter-to-pythonoperator-in-airflow
