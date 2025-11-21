# Hệ thống Real-time Streaming với Kafka cho Crowd Monitoring

Hệ thống này xây dựng một pipeline real-time để xử lý video frames và đếm số người sử dụng Kafka, PostgreSQL, và web dashboard.

## Recent updates

- **Docker Compose**: thêm Kafka broker thứ 2 để chạy replication_factor=2, mọi service đều connect cả `kafka:29092` và `kafka-2:29093`. Thêm producer phụ (`producer-quad`). Trong `docker-compose.yml` không nêu port cho services và container name, các cấu hình port chạy local thì đặt trong `docker-compose.override.yml` (không commit) để tránh xung đột dự án khác.
- **Soft pause pipeline**: `db/init.sql` tạo bảng `pipeline_control` (không còn seed cũ). Producers và consumer đều kiểm tra flag này; Airflow DAG có thêm tasks `pause_pipeline` / `resume_pipeline` để thao tác.
- **Producers**: dc sửa thành chỉ gửi frame thô (không chạy YOLO). Có 2 cấu hình video (`config.yaml`, `config_quad_cam.yaml`). Mỗi producer đọc `pipeline_control` trong PostgreSQL để dừng/tái hoạt động khi cần.
- **Consumer**: kết nối cả 2 Kafka broker, tôn trọng flag pause, tiếp tục xử lý YOLO và ghi kết quả vào PostgreSQL như trước.


## Kiến trúc hệ thống

- **Kafka + Zookeeper**: Message broker cho streaming data
- **PostgreSQL**: Lưu trữ kết quả crowd counting
- **Producer Service**: Xử lý video, extract frames, gửi đến Kafka
- **Web Dashboard**: Hiển thị real-time statistics và charts
- **Airflow**: Monitoring và alerting

## Yêu cầu hệ thống

- Docker & Docker Compose
- Python 3.9+
- 4GB RAM (recommended)
- 10GB disk space

## Cài đặt và chạy

### 1. Clone repository
```bash
git clone <repository-url>
cd crowd-monitoring-system
```

### 2. Chuẩn bị video sample (tùy chọn)
Đặt file video `sample_video.mp4` vào thư mục `producer/` để test Producer.
Nếu không có, Producer sẽ sử dụng camera mặc định.

### 3. Chạy hệ thống
```bash
# Build và start tất cả services
docker-compose up -d

# Xem logs
docker-compose logs -f

# Stop hệ thống
docker-compose down
```

### 4. Truy cập Dashboard
- **Web Dashboard**: http://localhost:5000
- **Airflow UI**: http://localhost:8080 (username: admin, password: admin)

## Cấu trúc project

```
crowd-monitoring-system/
├── docker-compose.yml          # Docker services configuration
├── producer/                   # Video producer service
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.yaml
│   └── producer.py
├── dashboard/                  # Web dashboard service
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py
│   ├── templates/
│   │   └── index.html
│   └── static/
│       ├── js/app.js
│       └── css/style.css
├── db/                         # Database initialization
│   └── init.sql
├── airflow/                    # Monitoring DAGs
│   └── crowd_monitoring_dag.py
└── README.md
```

## API Endpoints

### Dashboard APIs
- `GET /api/statistics` - Thống kê theo location và time range
- `GET /api/latest` - 50 records mới nhất
- `GET /api/chart-data` - Dữ liệu cho charts
- `GET /api/summary` - Summary tổng quan

## Cấu hình

### Producer Configuration (`producer/config.yaml`)
```yaml
video_source: "sample_video.mp4"  # File path hoặc camera index
frame_interval: 1.0               # Giây giữa các frames
kafka_broker: "kafka:29092"       # Kafka broker address
topic: "video_frames"             # Kafka topic
location_id: "location_1"         # ID location
max_retries: 5                    # Số lần retry
retry_delay: 2                    # Delay giữa retries (giây)
```

### Environment Variables
Các service sử dụng environment variables được định nghĩa trong `docker-compose.yml`:
- Database: `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- Kafka: `KAFKA_BROKER`

## Monitoring và Alerting

### Airflow DAG
DAG `crowd_monitoring_dag` chạy mỗi 5 phút để:
1. Kiểm tra kết nối Kafka
2. Monitor health của Producer
3. Theo dõi lag của Kafka topic
4. Kiểm tra health của Dashboard
5. Health check tổng thể và alert

### Logging
- Tất cả services ghi log chi tiết
- Producer logs: `producer.log`
- Dashboard logs: container logs
- Airflow logs: `/opt/airflow/logs`

## Troubleshooting

### Services không start
```bash
# Kiểm tra status
docker-compose ps

# Xem logs của service cụ thể
docker-compose logs kafka
docker-compose logs producer
```

### Producer không gửi messages
```bash
# Kiểm tra Kafka topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic video_frames --from-beginning

# Kiểm tra Producer logs
docker-compose logs producer
```

### Dashboard không load data
```bash
# Kiểm tra database connection
docker exec -it postgres psql -U crowd_user -d crowd_db -c "SELECT COUNT(*) FROM crowd_results;"

# Kiểm tra Dashboard logs
docker-compose logs dashboard
```

### Reset hệ thống
```bash
# Stop và xóa tất cả
docker-compose down -v

# Rebuild
docker-compose up --build -d
```

## Performance Tuning

### Kafka
- `num.partitions`: 3 (configured in DAG)
- `replication.factor`: 1 (single node setup)

### Database
- Indexes trên: `timestamp`, `location_id`, `frame_id`
- Partitioning có thể thêm cho dữ liệu lớn

### Producer
- `frame_interval`: Điều chỉnh dựa trên nhu cầu real-time
- Batch sending có thể implement để tối ưu throughput

## Development

### Thêm location mới
1. Update `config.yaml` trong producer
2. Restart producer service
3. Update dashboard filters nếu cần

### Extend APIs
- Thêm endpoints mới trong `dashboard/app.py`
- Update frontend trong `static/js/app.js`

### Custom monitoring
- Modify `airflow/crowd_monitoring_dag.py`
- Thêm alerting (email, Slack, etc.)

## License

This project is for educational purposes.

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## Support

Nếu gặp vấn đề, kiểm tra:
1. Docker và Docker Compose đã cài đặt
2. Ports 5000, 8080, 9092, 5432 không bị conflict
3. Đủ RAM và disk space
4. Firewall không block các ports
