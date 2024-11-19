from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.compute import ComputeEngineStartInstanceOperator, ComputeEngineStopInstanceOperator
from datetime import datetime

DESTINATION_BUCKET_NAME = "test_bucket_hakjoon"
FILE_NAME = "new_file.xlsx"


GCP_CONN_ID = "google_cloud_default"   # Airflow의 GCP 연결 ID
PROJECT_ID = "hakjoon-hyundai-test"              # GCP 프로젝트 ID
REGION = " asia-northeast3"                 # Cloud Run이 실행될 GCP 리전
CLOUD_RUN_JOB_NAME = "nas-to-gcs" # 실행할 Cloud Run Job 이름



with DAG(
    dag_id="example_gcs_sensor",
    schedule_interval=None,
    start_date=datetime(2024, 11, 18),
    catchup=False,
) as dag:

    # sense_files_in_folder = GCSObjectsWithPrefixExistenceSensor(
    #     task_id='sense_files_in_folder',
    #     bucket='your-gcs-bucket',           # 감지할 GCS 버킷 이름
    #     prefix='your/folder/path/',          # 감지할 폴더 경로
    #     google_cloud_conn_id='google_cloud_default'  # GCP 연결 ID
    # )
    
    # pull_messages_async = PubSubPullSensor(
    # task_id="pull_messages_async",
    # ack_messages=True,
    # project_id=PROJECT_ID,
    # subscription=subscription,
    # deferrable=True,
    # )
    
    # gcs_object_exists = GCSObjectExistenceSensor(
    #     bucket=DESTINATION_BUCKET_NAME,
    #     object=FILE_NAME,
    #     task_id="gcs_object_exists_task",
    # )

    # stop_instance = ComputeEngineStopInstanceOperator(
    #     task_id='stop_instance',
    #     project_id='your-gcp-project-id',  # GCP 프로젝트 ID
    #     zone='your-instance-zone',         # 인스턴스가 있는 GCP Zone (예: 'us-central1-a')
    #     resource_id='your-instance-name',  # 중지할 인스턴스 이름
    #     gcp_conn_id='google_cloud_default' # 연결 설정에서 사용한 Connection ID
    # )
    

    run_cloud_run_job = CloudRunExecuteJobOperator(
        task_id="run_cloud_run_job",
        location=REGION,
        job_id=CLOUD_RUN_JOB_NAME,
        project_id=PROJECT_ID,
        google_cloud_conn_id=GCP_CONN_ID,
        wait_for_completion=True,  # 작업이 완료될 때까지 대기
    )

    # delete_gcs_object = GCSDeleteObjectsOperator(
    #     task_id="delete_gcs_object",
    #     bucket_name=DESTINATION_BUCKET_NAME,
    #     objects=FILE_NAME,
    #     google_cloud_conn_id=GCP_CONN_ID,
    # )