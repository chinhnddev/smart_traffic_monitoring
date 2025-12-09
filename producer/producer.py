import base64
import cv2
import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime

import yaml
import psycopg2
from kafka import KafkaProducer
from kafka.errors import KafkaError
from yt_dlp import YoutubeDL

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('producer.log')
    ]
)
logger = logging.getLogger(__name__)


class CrowdMonitoringProducer:
    """Producer chỉ chịu trách nhiệm gửi frame thô vào Kafka."""

    def __init__(self, config_path=None):
        config_file = config_path or os.environ.get("PRODUCER_CONFIG_PATH", "config.yaml")
        with open(config_file, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # Video configuration
        self.video_source = self.config["video_source"]
        self.youtube_url = self.config.get("youtube_url", "")
        self.frame_interval = self.config.get("frame_interval", 1.0)

        # Kafka configuration
        raw_brokers = self.config['kafka_broker']
        self.kafka_brokers = [broker.strip() for broker in raw_brokers.split(',') if broker.strip()]
        self.topic = self.config['topic']
        self.location_id = self.config['location_id']
        self.max_retries = self.config['max_retries']
        self.retry_delay = self.config['retry_delay']

        # Database configuration for pipeline control flag
        self.db_host = self.config.get('db_host', 'postgres')
        self.db_port = self.config.get('db_port', 5432)
        self.db_name = self.config.get('db_name', 'crowd_db')
        self.db_user = self.config.get('db_user', 'crowd_user')
        self.db_password = self.config.get('db_password', 'crowd_pass')

        # Runtime state
        self.producer = None
        self.cap = None
        self.running = True
        self.pipeline_paused = False

        # Thiết lập signal handler cho graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Xử lý tín hiệu để tắt graceful"""
        logger.info("Nhận tín hiệu shutdown, đang dừng Producer...")
        self.running = False

    def connect_kafka(self):
        """Kết nối đến Kafka với retry logic"""
        retries = 0
        while retries < self.max_retries and self.running:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_brokers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    retry_backoff_ms=1000
                )
                logger.info(f"Kết nối thành công đến Kafka brokers: {', '.join(self.kafka_brokers)}")
                return True
            except Exception as e:
                retries += 1
                logger.error(
                    f"Lỗi kết nối Kafka (thử lại {retries}/{self.max_retries}): {e}"
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
        return False

    def get_youtube_stream_url(self):
        """Lấy URL stream từ YouTube video"""
        try:
            ydl_opts = {
                'format': 'best[height<=720]',  # Chọn chất lượng thấp hơn để xử lý nhanh
                'quiet': True,
                'no_warnings': True,
            }
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(self.youtube_url, download=False)
                return info["url"]
        except Exception as e:
            logger.error(f"Lỗi lấy YouTube stream URL: {e}")
            return None

    def open_video_source(self):
        """Mở nguồn video (YouTube, file hoặc camera)"""
        try:
            if self.video_source == "youtube":
                # YouTube stream
                stream_url = self.get_youtube_stream_url()
                if not stream_url:
                    return False
                self.cap = cv2.VideoCapture(stream_url)
                logger.info("Mở YouTube stream thành công.")
            elif self.video_source.isdigit():
                self.cap = cv2.VideoCapture(int(self.video_source))
                logger.info(f"Mở camera index {self.video_source}")
            else:
                if not os.path.exists(self.video_source):
                    logger.error(f"Video file không tồn tại: {self.video_source}")
                    return False
                self.cap = cv2.VideoCapture(self.video_source)
                logger.info(f"Mở video file: {self.video_source}")

            if not self.cap.isOpened():
                logger.error(f"Không thể mở video source: {self.video_source}")
                return False

            logger.info("Mở thành công video source")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi mở video source: {e}")
            return False

    @staticmethod
    def encode_frame(frame):
        success, buffer = cv2.imencode(".jpg", frame)
        if not success:
            raise ValueError("Không thể encode frame sang JPEG")
        return base64.b64encode(buffer).decode("utf-8")

    def send_frame_to_kafka(self, frame_b64, frame_id, timestamp):
        message = {
            "frame_bytes": frame_b64,
            "timestamp": timestamp,
            "frame_id": frame_id,
            "location_id": self.location_id,
        }

        retries = 0
        while retries < self.max_retries and self.running:
            try:
                future = self.producer.send(self.topic, value=message, key=self.location_id)
                record_metadata = future.get(timeout=10)
                logger.info(
                    f"Đã gửi frame {frame_id} đến topic {record_metadata.topic} partition {record_metadata.partition}"
                )
                return True
            except KafkaError as e:
                retries += 1
                logger.error(
                    f"Lỗi gửi Kafka (thử lại {retries}/{self.max_retries}): {e}"
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Lỗi không mong muốn khi gửi Kafka: {e}")
                break
        return False

    def fetch_pipeline_status(self):
        try:
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status FROM pipeline_control ORDER BY id DESC LIMIT 1"
            )
            status = cursor.fetchone()
            cursor.close()
            conn.close()
            return status[0] if status else "RUNNING"
        except Exception as e:
            logger.error("Không đọc được pipeline status: %s", e)
            return "RUNNING"

    def process_video(self):
        frame_count = 0
        last_frame_time = 0

        while self.running:
            status = self.fetch_pipeline_status()
            if status == "PAUSED":
                if not self.pipeline_paused:
                    logger.info("Pipeline đang PAUSED – tạm dừng gửi frame")
                    self.pipeline_paused = True
                time.sleep(2)
                continue
            else:
                if self.pipeline_paused:
                    logger.info("Pipeline RESUMED – tiếp tục gửi frame")
                    self.pipeline_paused = False

            ret, frame = self.cap.read()
            if not ret:
                logger.info("Kết thúc video hoặc không thể đọc frame.")
                break

            current_time = time.time()
            if current_time - last_frame_time < self.frame_interval:
                time.sleep(0.01)
                continue

            try:
                frame_b64 = self.encode_frame(frame)
                frame_id = f"{self.location_id}-{uuid.uuid4()}"
                timestamp = datetime.utcnow().isoformat() + "Z"

                if self.send_frame_to_kafka(frame_b64, frame_id, timestamp):
                    frame_count += 1
                    logger.info(f"Đã gửi frame {frame_count} (ID: {frame_id})")
                else:
                    logger.error("Không thể gửi frame lên Kafka.")

                last_frame_time = current_time
            except Exception as e:
                logger.error(f"Lỗi xử lý frame: {e}")

        logger.info(f"Tổng số frame đã gửi: {frame_count}")

    def run(self):
        logger.info("Bắt đầu Producer (gửi frame thô).")

        if not self.connect_kafka():
            logger.error("Không thể kết nối Kafka. Thoát.")
            return

        if not self.open_video_source():
            logger.error("Không thể mở video source. Thoát.")
            return

        try:
            self.process_video()
        except Exception as e:
            logger.error(f"Lỗi runtime Producer: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Dọn dẹp tài nguyên"""
        if self.cap:
            self.cap.release()
            logger.info("Đã đóng video capture.")

        if self.producer:
            self.producer.close()
            logger.info("Đã đóng Kafka producer.")

        cv2.destroyAllWindows()
        logger.info("Producer đã dừng.")


if __name__ == "__main__":
    CrowdMonitoringProducer().run()
