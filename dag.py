import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

from util.get_geodata import download_land_cover, download_aoi, generate_labels_geodata
from util.generate_satellite_images import get_satellite_images
from util.generate_files_path_csvs import generate_fpath_csvs

VARIABLES = Variable.get("variables_config", deserialize_json=True)

EMAIL = VARIABLES["email"]
LANDCOVER_URL = VARIABLES["landcover_url"]
AOI_GEOJSON_URL = VARIABLES["aoi_geojson_url"]
SENTINELHUB_INSTANCE_ID = VARIABLES["sentinelhub_instance_id"]
CRS_OUTPUT = VARIABLES["crs_output"]

GCP_PROJECT = VARIABLES["project_id"]
GCS_OUTPUT_BUCKET = VARIABLES["bucket_output"]
GCS_OUTPUT_PATH = 'gs://{0}'.format(GCS_OUTPUT_BUCKET)
GCP_REGION = VARIABLES["region"]
GCP_ZONE = VARIABLES["zone"]

TFREC_OUTPUT_DIR = os.path.join(GCS_OUTPUT_PATH, 'tfrec_imgs')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 29),
    'email': EMAIL,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'project_id': GCP_PROJECT
}

dag = DAG(
    'generate_land_segmentation_training_data',
    default_args=default_args,
    description=('pipeline for generating training data '
                 'for a land segmentation ML model '
                 'on Sentinel2 satellite images'),
    schedule_interval=None,
)

t1 = BashOperator(
    task_id='set_sentinelhub_instance_id',
    bash_command='sudo chmod 666 /opt/python3.6/lib/python3.6/site-packages/sentinelhub/config.json && '
                 'sentinelhub.config --instance_id {{ params.instance_id }}',
    params={'instance_id':SENTINELHUB_INSTANCE_ID},
    dag=dag
)

t2 = PythonOperator(
    task_id='download_land_cover',
    python_callable=download_land_cover,
    op_kwargs={'landcover_url': LANDCOVER_URL},
    dag=dag
)

t3 = PythonOperator(
    task_id='download_aoi',
    python_callable=download_aoi,
    op_kwargs={'aoi_geojson_url': AOI_GEOJSON_URL},
    dag=dag
)

t4 = PythonOperator(
    task_id='generate_labels_geodata',
    python_callable=generate_labels_geodata,
    op_kwargs={
        'codes_to_keep': list(range(12, 27)),
        'crs_output': CRS_OUTPUT
    },
    dag=dag
)

t5 = PythonOperator(
    task_id='get_satellite_images',
    python_callable=get_satellite_images,
    op_kwargs={
        'resolution': 10,
        'time_interval': ('2017-01-01', '2017-12-31'),
        'crs_output': CRS_OUTPUT,
        'img_size': 256,
        'output_bucket': GCS_OUTPUT_BUCKET
    },
    dag=dag
)

t6 = PythonOperator(
    task_id='generate_fpath_csvs',
    python_callable=generate_fpath_csvs,
    op_kwargs={
        'gcs_bucket_dir': GCS_OUTPUT_PATH,
        'rand_seed': 2,
        'split': 0.8
    },
    dag=dag
)

t7 = DataFlowPythonOperator(
    task_id='convert_npy_image_files_to_tfrecords',
    py_file='/home/airflow/gcs/dags/dataflow/npy_to_tfrec.py',
    dataflow_default_options={
        'project': GCP_PROJECT,
        'zone': GCP_ZONE,
        'staging_location':os.path.join(GCS_OUTPUT_PATH, 'tmp', 'staging'),
        'temp_location':os.path.join(GCS_OUTPUT_PATH, 'tmp'),
        'runner':'DataflowRunner',
        'job_name':'preprocess-images-' + datetime.now().strftime('%y%m%d-%H%M%S'),
        'teardown_policy': 'TEARDOWN_ALWAYS'
    },
    options={
        'train_csv': os.path.join(GCS_OUTPUT_PATH, 'train_fpaths.csv'),
        'validation_csv': os.path.join(GCS_OUTPUT_PATH, 'valid_fpaths.csv'),
        'output_dir': TFREC_OUTPUT_DIR
    },
    gcp_conn_id='google_cloud_default',
    dag=dag
)


# Tasks dependencies
t1 >> t2
t2 >> t3
t3 >> t4
t4 >> t5
t5 >> t6
t6 >> t7
