import cv2
import json
import logging
import time
import uuid
from datetime import datetime
import yaml
import sys
import signal
import os
import numpy as np
from scipy.ndimage import gaussian_filter
import psycopg2
from ultralytics import YOLO
from torch.serialization import add_safe_globals
from ultralytics.nn.tasks import DetectionModel
import torch.nn as nn
from ultralytics.nn.modules import Conv
from yt_dlp import YoutubeDL
from playsound import playsound
from kafka import KafkaProducer
from kafka.errors import KafkaError

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
    def __init__(self, config_path='config.yaml'):
        # Đọc cấu hình từ file
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        # Video configuration
        self.video_source = self.config['video_source']
        self.youtube_url = self.config.get('youtube_url', '')
        self.frame_interval = self.config['frame_interval']

        # Kafka configuration
        self.kafka_broker = self.config['kafka_broker']
        self.topic = self.config['topic']
        self.location_id = self.config['location_id']
        self.max_retries = self.config['max_retries']
        self.retry_delay = self.config['retry_delay']

        # YOLO configuration
        self.yolo_model_path = self.config['yolo_model']
        self.confidence_threshold = self.config['confidence_threshold']
        self.person_class_id = self.config['person_class_id']

        # Alert configuration
        self.alert_threshold = self.config['alert_threshold']
        self.alert_cooldown = self.config['alert_cooldown']
        self.alert_sound = self.config.get('alert_sound', None)

        # Visualization configuration
        self.show_video = self.config['show_video']
        self.heatmap_alpha = self.config['heatmap_alpha']
        self.info_box_color = tuple(self.config['info_box_color'])
        self.info_box_thickness = self.config['info_box_thickness']

        # Database configuration
        self.db_config = {
            'host': self.config['db_host'],
            'port': self.config['db_port'],
            'database': self.config['db_name'],
            'user': self.config['db_user'],
            'password': self.config['db_password']
        }

        # Initialize components
        self.producer = None
        self.cap = None
        self.model = None
        self.db_conn = None
        self.running = True
        self.last_alert_time = 0

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
                    bootstrap_servers=[self.kafka_broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    retry_backoff_ms=1000
                )
                logger.info(f"Kết nối thành công đến Kafka broker: {self.kafka_broker}")
                return True
            except Exception as e:
                retries += 1
                logger.error(f"Lỗi kết nối Kafka (thử lại {retries}/{self.max_retries}): {e}")
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
                return info['url']
        except Exception as e:
            logger.error(f"Lỗi khi lấy YouTube stream URL: {e}")
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
                logger.info(f"Mở YouTube stream: {self.youtube_url}")
            elif self.video_source.isdigit():
                # Camera
                self.cap = cv2.VideoCapture(int(self.video_source))
                logger.info(f"Mở camera: {self.video_source}")
            else:
                # Video file
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

    def generate_heatmap(self, detections, frame_shape):
        """Tạo density heatmap từ detections"""
        heatmap = np.zeros((frame_shape[0], frame_shape[1]), dtype=np.float32)

        for det in detections:
            x1, y1, x2, y2 = det[:4]
            center_x = int((x1 + x2) / 2)
            center_y = int((y1 + y2) / 2)

            # Thêm điểm vào heatmap
            if 0 <= center_x < frame_shape[1] and 0 <= center_y < frame_shape[0]:
                heatmap[center_y, center_x] += 1

        # Áp dụng Gaussian blur để tạo density map
        heatmap = gaussian_filter(heatmap, sigma=15)

        # Normalize và chuyển thành colormap
        if heatmap.max() > 0:
            heatmap = heatmap / heatmap.max()

        heatmap_colored = cv2.applyColorMap(np.uint8(255 * heatmap), cv2.COLORMAP_JET)
        return heatmap_colored

    def draw_info_box(self, frame, crowd_count, density_level):
        """Vẽ info box với thông tin crowd"""
        h, w = frame.shape[:2]

        # Tạo text
        info_text = f"People: {crowd_count}"
        density_text = f"Density: {density_level}"

        # Tính kích thước text
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.7
        thickness = 2

        (text_w, text_h), _ = cv2.getTextSize(info_text, font, font_scale, thickness)
        (density_w, density_h), _ = cv2.getTextSize(density_text, font, font_scale, thickness)

        # Vị trí box
        box_w = max(text_w, density_w) + 20
        box_h = text_h + density_h + 30
        box_x = w - box_w - 10
        box_y = 10

        # Vẽ box
        cv2.rectangle(frame, (box_x, box_y), (box_x + box_w, box_y + box_h),
                     self.info_box_color, self.info_box_thickness)

        # Vẽ text
        cv2.putText(frame, info_text, (box_x + 10, box_y + text_h + 10),
                   font, font_scale, (255, 255, 255), thickness)
        cv2.putText(frame, density_text, (box_x + 10, box_y + text_h + density_h + 20),
                   font, font_scale, (255, 255, 255), thickness)

        return frame

    def trigger_alert(self, crowd_count):
        """Trigger alert khi crowd count vượt ngưỡng"""
        current_time = time.time()
        if current_time - self.last_alert_time >= self.alert_cooldown:
            logger.warning(f"ALERT: High crowd count detected: {crowd_count} people")

            # Phát âm thanh cảnh báo
            if self.alert_sound and os.path.exists(self.alert_sound):
                try:
                    playsound(self.alert_sound, block=False)
                except Exception as e:
                    logger.error(f"Lỗi phát âm thanh cảnh báo: {e}")

            self.last_alert_time = current_time
            return True
        return False

    def save_to_database(self, crowd_count, density_level, timestamp):
        """Lưu kết quả vào database"""
        try:
            with self.db_conn.cursor() as cursor:
                frame_id = f"{self.location_id}-{uuid.uuid4()}"
                cursor.execute("""
                    INSERT INTO crowd_results (timestamp, location_id, frame_id, crowd_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (frame_id) DO UPDATE SET
                        crowd_count = EXCLUDED.crowd_count,
                        timestamp = EXCLUDED.timestamp,
                        location_id = EXCLUDED.location_id
                """, (timestamp, self.location_id, frame_id, crowd_count))
                self.db_conn.commit()
            logger.info(f"Lưu vào DB: {crowd_count} people")
            return True
        except Exception as e:
            logger.error(f"Lỗi lưu vào database: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return False

    def send_crowd_data_to_kafka(self, crowd_count, density_level, timestamp):
        """Gửi dữ liệu crowd đến Kafka"""
        message = {
            "timestamp": timestamp,
            "location_id": self.location_id,
            "crowd_count": crowd_count,
            "density_level": density_level,
            "alert_triggered": crowd_count >= self.alert_threshold
        }

        retries = 0
        while retries < self.max_retries and self.running:
            try:
                future = self.producer.send(self.topic, value=message, key=self.location_id)
                record_metadata = future.get(timeout=10)
                logger.info(f"Gửi crowd data thành công đến topic {record_metadata.topic}")
                return True
            except KafkaError as e:
                retries += 1
                logger.error(f"Lỗi gửi Kafka (thử lại {retries}/{self.max_retries}): {e}")
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Lỗi không mong muốn: {e}")
                break
        return False

    def process_video(self):
        """Xử lý video với YOLO detection và gửi kết quả"""
        frame_count = 0
        last_frame_time = time.time()

        while self.running:
            try:
                ret, frame = self.cap.read()
                if not ret:
                    logger.info("Kết thúc video hoặc không thể đọc frame")
                    break

                current_time = time.time()
                if current_time - last_frame_time >= self.frame_interval:
                    # YOLO detection
                    results = self.model(frame, conf=self.confidence_threshold, classes=[self.person_class_id])
                    detections = results[0].boxes.xyxy.cpu().numpy() if len(results[0].boxes) > 0 else []

                    crowd_count = len(detections)

                    # Tính density level dựa trên số người và diện tích frame
                    frame_area = frame.shape[0] * frame.shape[1]
                    density_level = crowd_count / (frame_area / 100000)  # people per 100k pixels

                    # Tạo timestamp
                    timestamp = datetime.utcnow().isoformat() + 'Z'

                    # Visualization
                    if self.show_video:
                        # Vẽ bounding boxes
                        for det in detections:
                            x1, y1, x2, y2 = det[:4].astype(int)
                            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)

                        # Tạo và overlay heatmap
                        if len(detections) > 0:
                            heatmap = self.generate_heatmap(detections, frame.shape)
                            frame = cv2.addWeighted(frame, 1 - self.heatmap_alpha, heatmap, self.heatmap_alpha, 0)

                        # Vẽ info box
                        frame = self.draw_info_box(frame, crowd_count, f"{density_level:.2f}")

                        # Hiển thị frame
                        cv2.imshow('Crowd Monitoring', frame)
                        if cv2.waitKey(1) & 0xFF == ord('q'):
                            self.running = False
                            break

                    # Trigger alert nếu cần
                    if crowd_count >= self.alert_threshold:
                        self.trigger_alert(crowd_count)

                    # Lưu vào database
                    self.save_to_database(crowd_count, density_level, timestamp)

                    # Gửi đến Kafka
                    if self.send_crowd_data_to_kafka(crowd_count, density_level, timestamp):
                        frame_count += 1
                        logger.info(f"Đã xử lý frame {frame_count}: {crowd_count} people, density: {density_level:.2f}")
                    else:
                        logger.error(f"Không thể gửi crowd data")

                    last_frame_time = current_time

                # Thêm delay nhỏ để tránh CPU usage cao
                time.sleep(0.01)

            except Exception as e:
                logger.error(f"Lỗi trong quá trình xử lý video: {e}")
                break

        logger.info(f"Tổng số frames đã xử lý: {frame_count}")

    def run(self):
        """Chạy Crowd Monitoring Producer"""
        logger.info("Bắt đầu Crowd Monitoring Producer...")

        if not self.connect_kafka():
            logger.error("Không thể kết nối đến Kafka. Thoát.")
            return

        if not self.connect_database():
            logger.error("Không thể kết nối đến database. Thoát.")
            return

        if not self.load_yolo_model():
            logger.error("Không thể load YOLO model. Thoát.")
            return

        if not self.open_video_source():
            logger.error("Không thể mở video source. Thoát.")
            return

        try:
            self.process_video()
        except Exception as e:
            logger.error(f"Lỗi trong quá trình chạy Producer: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Dọn dẹp tài nguyên"""
        if self.cap:
            self.cap.release()
            logger.info("Đã đóng video capture")

        if self.producer:
            self.producer.close()
            logger.info("Đã đóng Kafka producer")

        if self.db_conn:
            self.db_conn.close()
            logger.info("Đã đóng database connection")

        if self.show_video:
            cv2.destroyAllWindows()
            logger.info("Đã đóng video windows")

        logger.info("Producer đã dừng")

if __name__ == "__main__":
    producer = CrowdMonitoringProducer()
    producer.run()
