from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.utils.dates import days_ago

import logging

import requests
import subprocess
import time
from functools import partial



logger = logging.getLogger(__name__)

COMPOSE_PROJECT_NAME = "crowd-analysis"
KAFKA_BROKERS = ['kafka:29092', 'kafka-2:29093']
PRODUCER_SERVICES = ['producer', 'producer-quad']



# Default arguments cho DAG

default_args = {

    'owner': 'crowd_monitoring',

    'depends_on_past': False,

    'start_date': days_ago(1),

    'email_on_failure': False,

    'email_on_retry': False,

    'retries': 3,
    
    'retry_delay': timedelta(minutes=5),

}



# Tạo DAG

dag = DAG(

    'crowd_monitoring_dag',

    default_args=default_args,

    description='DAG để monitor hệ thống crowd monitoring',

    schedule_interval=timedelta(minutes=5),  # Chạy mỗi 5 phút

    catchup=False,

    tags=['crowd', 'monitoring', 'kafka'],

)



def check_kafka_connection():

    """Task 1: Kiểm tra kết nối Kafka"""

    try:

        # Sử dụng kafka-python để kiểm tra kết nối

        from kafka import KafkaAdminClient



        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,

            client_id='airflow_monitor'

        )



        # Lấy danh sách topics

        topics = admin_client.list_topics()

        logger.info(f"Kafka connection successful. Topics: {topics}")



        # Kiểm tra topic 'video_frames' có tồn tại không

        if 'video_frames' not in topics:

            logger.warning("Topic 'video_frames' không tồn tại")

            # Tạo topic nếu chưa có

            from kafka.admin import NewTopic

            topic = NewTopic(name='video_frames', num_partitions=3, replication_factor=2)

            admin_client.create_topics([topic])

            logger.info("Đã tạo topic 'video_frames'")



        admin_client.close()

        return "Kafka connection OK"



    except Exception as e:

        logger.error(f"Kafka connection failed: {e}")

        raise e



def _container_name(service_name: str) -> str:
    return f"{COMPOSE_PROJECT_NAME}-{service_name}-1"


def check_container_health(service_name: str):
    """Generic container health check."""
    container = _container_name(service_name)
    try:
        result = subprocess.run(
            ['docker', 'ps', '--filter', f'name={container}', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            check=True
        )

        status = result.stdout.strip()
        if 'Up' in status:
            logger.info("Container %s is running (%s)", container, status)
            return f"{service_name} healthy"

        logger.error("Container %s is not running", container)
        subprocess.run(['docker', 'restart', container], check=True)
        logger.info("Attempted to restart %s", container)
        return f"{service_name} restarted"

    except Exception as e:
        logger.error("Error checking %s health: %s", container, e)
        raise e



def monitor_kafka_topic_lag():

    """Task 3: Monitor lag của Kafka topic"""

    try:

        from kafka import KafkaConsumer, TopicPartition



        consumer = KafkaConsumer(
            'video_frames',
            bootstrap_servers=KAFKA_BROKERS,

            group_id='airflow_monitor_group',

            auto_offset_reset='earliest',

            enable_auto_commit=False

        )



        # Lấy thông tin partitions

        partitions = consumer.partitions_for_topic('video_frames')

        if not partitions:

            logger.warning("No partitions found for topic 'video_frames'")

            consumer.close()

            return "No partitions to monitor"



        total_lag = 0

        partition_count = 0



        for partition in partitions:

            tp = TopicPartition('video_frames', partition)

            # Lấy end offset

            end_offsets = consumer.end_offsets([tp])

            end_offset = end_offsets[tp]



            # Lấy committed offset

            committed = consumer.committed(tp)

            committed_offset = committed.offset if committed else 0



            lag = end_offset - committed_offset

            total_lag += lag

            partition_count += 1



            logger.info(f"Partition {partition}: end={end_offset}, committed={committed_offset}, lag={lag}")



        consumer.close()



        avg_lag = total_lag / partition_count if partition_count > 0 else 0

        logger.info(f"Average lag across {partition_count} partitions: {avg_lag}")



        # Alert nếu lag quá cao

        if avg_lag > 1000:  # Threshold có thể điều chỉnh

            logger.warning(f"High lag detected: {avg_lag}")

            # Có thể gửi alert ở đây



        return f"Topic lag monitored: {avg_lag}"



    except Exception as e:

        logger.error(f"Error monitoring topic lag: {e}")

        raise e



def check_dashboard_health():

    """Task 4: Kiểm tra health của Dashboard"""

    try:

        # Kiểm tra HTTP response từ dashboard

        response = requests.get('http://dashboard:5000/api/summary', timeout=10)



        if response.status_code == 200:

            data = response.json()

            logger.info(f"Dashboard is healthy. Total frames: {data.get('total_frames', 0)}")

            return "Dashboard is healthy"

        else:

            logger.error(f"Dashboard returned status code: {response.status_code}")

            raise Exception(f"Dashboard unhealthy: {response.status_code}")



    except requests.exceptions.RequestException as e:

        logger.error(f"Error connecting to dashboard: {e}")

        raise e



def comprehensive_health_check():

    """Task 5: Health check tổng thể và alert nếu có lỗi"""

    try:

        issues = []



        # Kiểm tra tất cả services

        services = ['zookeeper', 'kafka', 'kafka-2', 'postgres', 'producer', 'producer-quad', 'consumer', 'dashboard', 'airflow-webserver', 'airflow-scheduler']



        for service in services:

            try:

                result = subprocess.run(

                    ['docker', 'ps', '--filter', f'name={service}', '--format', '{{.Status}}'],

                    capture_output=True,

                    text=True,

                    check=True

                )



                if 'Up' not in result.stdout:

                    issues.append(f"Service {service} is not running")

                    logger.error(f"Service {service} is down")



            except subprocess.CalledProcessError as e:

                issues.append(f"Error checking service {service}: {e}")

                logger.error(f"Error checking service {service}: {e}")



        # Kiểm tra database connection

        try:

            import psycopg2

            conn = psycopg2.connect(

                host='postgres',

                database='crowd_db',

                user='crowd_user',

                password='crowd_pass',

                port=5432

            )

            conn.close()

            logger.info("Database connection OK")

        except Exception as e:

            issues.append(f"Database connection failed: {e}")

            logger.error(f"Database connection failed: {e}")



        if issues:

            alert_message = f"Health check failed with {len(issues)} issues: {'; '.join(issues)}"

            logger.error(alert_message)

            # Có thể gửi email alert, Slack notification, etc.

            raise Exception(alert_message)

        else:

            logger.info("All services are healthy")

            return "All systems healthy"



    except Exception as e:

        logger.error(f"Comprehensive health check failed: {e}")

        raise e



# Định nghĩa các tasks

task_check_kafka = PythonOperator(

    task_id='check_kafka_connection',

    python_callable=check_kafka_connection,

    dag=dag,

)



task_check_producer = PythonOperator(
    task_id='check_producer_health',
    python_callable=partial(check_container_health, 'producer'),
    dag=dag,
)

task_check_producer_quad = PythonOperator(
    task_id='check_producer_quad_health',
    python_callable=partial(check_container_health, 'producer-quad'),
    dag=dag,
)


task_monitor_lag = PythonOperator(

    task_id='monitor_kafka_topic_lag',

    python_callable=monitor_kafka_topic_lag,

    dag=dag,

)



task_check_dashboard = PythonOperator(

    task_id='check_dashboard_health',

    python_callable=check_dashboard_health,

    dag=dag,

)



task_comprehensive_check = PythonOperator(
    task_id='comprehensive_health_check',
    python_callable=comprehensive_health_check,
    dag=dag,
)





def check_consumer_health():
    """Task: Kiểm tra tình trạng container consumer"""
    return check_container_health('consumer')


def verify_db_inserts():
    """Task: Kiểm tra các bản ghi mới được insert trong 10 phút gần đây"""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host='postgres',
            database='crowd_db',
            user='crowd_user',
            password='crowd_pass',
            port=5432
        )

        cursor = conn.cursor()
        query = """
            SELECT COUNT(*) AS new_records
            FROM crowd_results
            WHERE timestamp >= NOW() - INTERVAL '10 minutes'
        """
        cursor.execute(query)
        new_records = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        logger.info(f"Found {new_records} new records in the last 10 minutes")

        if new_records == 0:
            logger.warning("No new records found in the last 10 minutes - possible issue with consumer")
            return "Warning: No new records"

        return f"DB inserts verified: {new_records} new records"

    except Exception as e:
        logger.error(f"Error verifying DB inserts: {e}")
        raise e


task_check_consumer = PythonOperator(
    task_id='check_consumer_health',
    python_callable=check_consumer_health,
    dag=dag,
)


task_verify_db_inserts = PythonOperator(
    task_id='verify_db_inserts',
    python_callable=verify_db_inserts,
    dag=dag,
)


def pause_pipeline():
    """Set pipeline status to PAUSED."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='postgres',
            database='crowd_db',
            user='crowd_user',
            password='crowd_pass',
            port=5432
        )
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE pipeline_control
                    SET status = 'PAUSED', updated_at = NOW()
                    WHERE id = (SELECT id FROM pipeline_control ORDER BY id DESC LIMIT 1)
                """)
        logger.info("Pipeline status set to PAUSED")
        return "PAUSED"
    except Exception as e:
        logger.error("Failed to pause pipeline: %s", e)
        raise e


def resume_pipeline():
    """Set pipeline status to RUNNING."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='postgres',
            database='crowd_db',
            user='crowd_user',
            password='crowd_pass',
            port=5432
        )
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE pipeline_control
                    SET status = 'RUNNING', updated_at = NOW()
                    WHERE id = (SELECT id FROM pipeline_control ORDER BY id DESC LIMIT 1)
                """)
        logger.info("Pipeline status set to RUNNING")
        return "RUNNING"
    except Exception as e:
        logger.error("Failed to resume pipeline: %s", e)
        raise e


pause_task = PythonOperator(
    task_id='pause_pipeline',
    python_callable=pause_pipeline,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
)

resume_task = PythonOperator(
    task_id='resume_pipeline',
    python_callable=resume_pipeline,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
)


# Định nghĩa dependencies (chạy tuần tự)
producer_checks = [task_check_producer, task_check_producer_quad]

task_check_kafka >> producer_checks
producer_checks >> task_check_consumer
task_check_consumer >> task_monitor_lag >> task_verify_db_inserts >> task_check_dashboard >> task_comprehensive_check

# Utilities (pause/resume) ko phải thành phần chính nhưng có thể trigger thủ công để pause pipeline
pause_task
resume_task
