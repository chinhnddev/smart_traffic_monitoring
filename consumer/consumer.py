import cv2
import json
import logging
import time
import base64
import numpy as np
from datetime import datetime
import yaml
import sys
import signal
import os
import psycopg2
from psycopg2.extras import execute_values
from ultralytics import YOLO
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from collections import deque

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('consumer.log')
    ]
)
logger = logging.getLogger(__name__)

class CrowdMonitoringConsumer:
    def __init__(self, config_path='config.yaml'):
        # Đọc cấu hình từ file
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # Kafka configuration
        raw_brokers = self.config['kafka_broker']
        self.kafka_brokers = [broker.strip() for broker in raw_brokers.split(',') if broker.strip()]
        self.topic = self.config['topic']
        self.consumer_group = self.config['consumer_group']
        self.max_retries = self.config['max_retries']
        self.retry_delay = self.config['retry_delay']

        # YOLO configuration
        self.yolo_model_path = self.config['yolo_model']
        self.confidence_threshold = self.config['confidence_threshold']
        self.person_class_id = self.config['person_class_id']
        self.alert_threshold = self.config['alert_threshold']

        # Database configuration
        self.db_config = {
            'host': self.config['db_host'],
            'port': self.config['db_port'],
            'database': self.config['db_name'],
            'user': self.config['db_user'],
            'password': self.config['db_password']
        }

        # Processing configuration
        self.batch_size = self.config['batch_size']
        self.max_workers = self.config['max_workers']

        # Initialize components
        self.consumer = None
        self.model = None
        self.db_conn = None
        self.running = True
        self.pipeline_paused = False

        # Operational metrics tracking
        self.metrics = {
            'processed': 0,
            'failed': 0,
            'latency_samples': deque(maxlen=1000),  # Keep last 1000 samples
            'alerts': 0,
            'start_time': time.time(),
            'last_metrics_log': time.time(),
            'last_metrics_save': time.time()
        }
        self.metrics_log_interval = 60  # Log every 60 seconds
        self.metrics_save_interval = 300  # Save to DB every 5 minutes

        # Thiết lập signal handler cho graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Xử lý tín hiệu để tắt graceful"""
        logger.info("Nhận tín hiệu shutdown, đang dừng Consumer...")
        self.running = False

    def connect_kafka(self):
        """Kết nối đến Kafka với retry logic"""
        retries = 0
        while retries < self.max_retries and self.running:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.kafka_brokers,
                    group_id=self.consumer_group,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000
                )
                logger.info(f"Kết nối thành công đến Kafka brokers: {', '.join(self.kafka_brokers)}")
                return True
            except Exception as e:
                retries += 1
                logger.error(f"Lỗi kết nối Kafka (thử lại {retries}/{self.max_retries}): {e}")
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
        return False

    def connect_database(self):
        """Kết nối đến PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**self.db_config)
            logger.info("Kết nối thành công đến PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Lỗi kết nối database: {e}")
            return False

    def load_yolo_model(self):
        """Load YOLOv8 model"""
        try:
            # Since we trust the YOLO model source, use weights_only=False
            import torch
            # Temporarily patch torch.load to allow loading
            original_load = torch.load
            def patched_load(*args, **kwargs):
                kwargs['weights_only'] = False
                return original_load(*args, **kwargs)
            torch.load = patched_load
            self.model = YOLO(self.yolo_model_path)
            torch.load = original_load  # Restore original
            logger.info(f"Load thành công YOLO model: {self.yolo_model_path}")
            return True
        except Exception as e:
            logger.error(f"Lỗi load YOLO model: {e}")
            return False

    def decode_frame(self, frame_bytes_b64):
        """Decode base64 frame bytes thành numpy array"""
        try:
            # Decode base64
            frame_bytes = base64.b64decode(frame_bytes_b64)

            # Convert bytes thành numpy array
            nparr = np.frombuffer(frame_bytes, np.uint8)

            # Decode thành OpenCV image
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if frame is None:
                raise ValueError("Không thể decode frame từ bytes")

            return frame
        except Exception as e:
            logger.error(f"Lỗi decode frame: {e}")
            return None

    def detect_crowd(self, frame):
        """Detect số người trong frame sử dụng YOLO"""
        try:
            # Run YOLO detection
            results = self.model(frame, conf=self.confidence_threshold, classes=[self.person_class_id])

            # Extract detections
            if len(results[0].boxes) > 0:
                detections = results[0].boxes.xyxy.cpu().numpy()
                crowd_count = len(detections)
            else:
                detections = []
                crowd_count = 0

            return crowd_count, detections
        except Exception as e:
            logger.error(f"Lỗi trong quá trình detection: {e}")
            return 0, []

    def process_message(self, message):
        """Xử lý một message từ Kafka"""
        process_start_time = time.time()
        
        try:
            # Extract data từ message
            frame_bytes_b64 = message.get('frame_bytes')
            timestamp = message.get('timestamp')
            frame_id = message.get('frame_id')
            location_id = message.get('location_id')

            if not all([frame_bytes_b64, timestamp, frame_id, location_id]):
                logger.warning(f"Message thiếu thông tin cần thiết: {message}")
                self.metrics['failed'] += 1
                return None

            # Decode frame
            frame = self.decode_frame(frame_bytes_b64)
            if frame is None:
                logger.error(f"Không thể decode frame cho frame_id: {frame_id}")
                self.metrics['failed'] += 1
                return None

            # Detect crowd
            crowd_count, detections = self.detect_crowd(frame)

            # Calculate latency (from frame creation to now)
            latency_ms = (process_start_time - timestamp) * 1000
            self.metrics['latency_samples'].append(latency_ms)
            self.metrics['processed'] += 1
            
            # Track alerts (threshold )
            if crowd_count > self.alert_threshold:
                self.metrics['alerts'] += 1

            # Tạo result
            result = {
                'timestamp': timestamp,
                'location_id': location_id,
                'frame_id': frame_id,
                'crowd_count': crowd_count
            }

            logger.info(f"Đã xử lý frame {frame_id}: {crowd_count} người (latency: {latency_ms:.1f}ms)")
            return result

        except Exception as e:
            logger.error(f"Lỗi xử lý message: {e}")
            self.metrics['failed'] += 1
            return None

    def save_batch_to_database(self, results):
        """Lưu batch results vào database"""
        if not results:
            return True

        try:
            with self.db_conn.cursor() as cursor:
                # Prepare data for bulk insert (convert Unix timestamp to datetime)
                data = [(datetime.fromtimestamp(r['timestamp']), r['location_id'], r['frame_id'], r['crowd_count']) for r in results]

                # Bulk insert
                execute_values(cursor, """
                    INSERT INTO crowd_results (timestamp, location_id, frame_id, crowd_count)
                    VALUES %s
                    ON CONFLICT (frame_id) DO UPDATE SET
                        crowd_count = EXCLUDED.crowd_count,
                        timestamp = EXCLUDED.timestamp
                """, data)

                self.db_conn.commit()

            logger.info(f"Đã lưu {len(results)} records vào database")
            return True

        except Exception as e:
            logger.error(f"Lỗi lưu vào database: {e}")
            self.db_conn.rollback()
            return False

    def fetch_pipeline_status(self):
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT status FROM pipeline_control ORDER BY id DESC LIMIT 1"
                    )
                    status = cursor.fetchone()
                    return status[0] if status else 'RUNNING'
        except Exception as e:
            logger.error(f"Không đọc được pipeline status: {e}")
            return 'RUNNING'

    def calculate_metrics(self):
        """Calculate current metrics"""
        elapsed = time.time() - self.metrics['start_time']
        total_frames = self.metrics['processed'] + self.metrics['failed']
        
        # Throughput (FPS)
        throughput = self.metrics['processed'] / elapsed if elapsed > 0 else 0
        
        # Drop rate (%)
        drop_rate = (self.metrics['failed'] / total_frames * 100) if total_frames > 0 else 0
        
        # Latency metrics (ms)
        if self.metrics['latency_samples']:
            latency_avg = np.mean(self.metrics['latency_samples'])
            latency_p95 = np.percentile(self.metrics['latency_samples'], 95)
        else:
            latency_avg = latency_p95 = 0
        
        # Alert rate (%)
        alert_rate = (self.metrics['alerts'] / self.metrics['processed'] * 100) if self.metrics['processed'] > 0 else 0
        
        return {
            'throughput': throughput,
            'latency_avg': latency_avg,
            'latency_p95': latency_p95,
            'drop_rate': drop_rate,
            'alert_rate': alert_rate,
            'total_processed': self.metrics['processed'],
            'total_failed': self.metrics['failed'],
            'total_alerts': self.metrics['alerts']
        }

    def log_metrics(self):
        """Log metrics to console and log file"""
        metrics = self.calculate_metrics()
        
        logger.info(
            f"[METRICS] Throughput: {metrics['throughput']:.2f} FPS | "
            f"Latency: avg={metrics['latency_avg']:.1f}ms, p95={metrics['latency_p95']:.1f}ms | "
            f"Drop: {metrics['drop_rate']:.2f}% | "
            f"Alert: {metrics['alert_rate']:.1f}% ({metrics['total_alerts']} alerts) | "
            f"Processed: {metrics['total_processed']}, Failed: {metrics['total_failed']}"
        )
        
        self.metrics['last_metrics_log'] = time.time()

    def save_metrics_to_db(self):
        """Save metrics to operational_metrics table"""
        try:
            metrics = self.calculate_metrics()
            
            # Prepare data
            location_id = self.config.get('location_id', 'unknown')
            metadata = {
                'location_id': location_id,
                'window_duration_seconds': int(time.time() - self.metrics['start_time']),
                'total_processed': metrics['total_processed'],
                'total_failed': metrics['total_failed']
            }
            
            metrics_data = [
                ('throughput', metrics['throughput'], json.dumps(metadata)),
                ('latency_avg', metrics['latency_avg'], json.dumps(metadata)),
                ('latency_p95', metrics['latency_p95'], json.dumps(metadata)),
                ('drop_rate', metrics['drop_rate'], json.dumps(metadata)),
                ('alert_rate', metrics['alert_rate'], json.dumps({'location_id': location_id, 'total_alerts': metrics['total_alerts']}))
            ]
            
            with self.db_conn.cursor() as cursor:
                execute_values(cursor, """
                    INSERT INTO operational_metrics (metric_type, value, metadata)
                    VALUES %s
                """, metrics_data)
                self.db_conn.commit()
            
            logger.info(f"[METRICS] Đã lưu metrics vào database")
            self.metrics['last_metrics_save'] = time.time()
            
        except Exception as e:
            logger.error(f"Lỗi lưu metrics vào database: {e}")
            if self.db_conn:
                self.db_conn.rollback()

    def consume_messages(self):
        """Tiêu thụ messages từ Kafka và xử lý"""
        logger.info("Bắt đầu consume messages từ Kafka...")

        message_batch = []
        last_save_time = time.time()

        try:
            for message in self.consumer:
                if not self.running:
                    break

                status = self.fetch_pipeline_status()
                if status == 'PAUSED':
                    if not self.pipeline_paused:
                        logger.info("Pipeline đang PAUSED – tạm dừng consume")
                        self.pipeline_paused = True
                    time.sleep(2)
                    continue
                else:
                    if self.pipeline_paused:
                        logger.info("Pipeline RESUMED – tiếp tục consume")
                        self.pipeline_paused = False

                # Process message
                result = self.process_message(message.value)
                if result:
                    message_batch.append(result)

                # Save batch nếu đủ size hoặc đủ time
                current_time = time.time()
                if len(message_batch) >= self.batch_size or (current_time - last_save_time) >= 5.0:
                    if message_batch:
                        self.save_batch_to_database(message_batch)
                        message_batch = []
                        last_save_time = current_time

                # Log metrics định kỳ
                if (current_time - self.metrics['last_metrics_log']) >= self.metrics_log_interval:
                    self.log_metrics()

                # Save metrics to DB định kỳ
                if (current_time - self.metrics['last_metrics_save']) >= self.metrics_save_interval:
                    self.save_metrics_to_db()

        except Exception as e:
            logger.error(f"Lỗi trong quá trình consume: {e}")

        # Save remaining messages
        if message_batch:
            self.save_batch_to_database(message_batch)

        logger.info("Dừng consume messages")

    def run(self):
        """Chạy Consumer"""
        logger.info("Bắt đầu Crowd Monitoring Consumer...")

        if not self.connect_kafka():
            logger.error("Không thể kết nối đến Kafka. Thoát.")
            return

        if not self.connect_database():
            logger.error("Không thể kết nối đến database. Thoát.")
            return

        if not self.load_yolo_model():
            logger.error("Không thể load YOLO model. Thoát.")
            return

        try:
            self.consume_messages()
        except Exception as e:
            logger.error(f"Lỗi trong quá trình chạy Consumer: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Dọn dẹp tài nguyên"""
        # Log final metrics
        logger.info("Chạy Final Metrics Summary...")
        self.log_metrics()
        
        # Try to save final metrics to DB
        try:
            if self.db_conn:
                self.save_metrics_to_db()
        except Exception as e:
            logger.error(f"Lỗi lưu final metrics: {e}")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Đã đóng Kafka consumer")

        if self.db_conn:
            self.db_conn.close()
            logger.info("Đã đóng database connection")

        logger.info("Consumer đã dừng")

if __name__ == "__main__":
    consumer = CrowdMonitoringConsumer()
    consumer.run()
