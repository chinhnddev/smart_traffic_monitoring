# TODO: Implement YOLO-based Crowd Monitoring System

## Step 1: Update Dependencies
- [ ] Update producer/requirements.txt to include ultralytics (for YOLOv8), yt-dlp, numpy, scipy, playsound (for alerts)

## Step 2: Update Configuration
- [ ] Modify producer/config.yaml to add YouTube URL, YOLO model settings, alert thresholds, processing FPS

## Step 3: Rewrite Producer Logic
- [ ] Rewrite producer/producer.py to:
  - [ ] Use yt-dlp to extract YouTube stream URL
  - [ ] Load YOLOv8n model for person detection
  - [ ] Process frames at 1-2 FPS with person detection
  - [ ] Generate bounding boxes and info overlay
  - [ ] Generate density heatmap and overlay
  - [ ] Display processed video in real-time window
  - [ ] Send crowd count results to database instead of raw frames
  - [ ] Implement alert system with visual/audio notifications for high crowd counts

## Step 4: Update Database Schema
- [ ] Update db/init.sql to add fields for density_level and alert_status if needed

## Step 5: Update Dashboard for Real-time Updates
- [ ] Modify dashboard/app.py to support real-time updates if needed
- [ ] Update dashboard templates and JS for real-time visualization

## Step 6: Testing and Verification
- [ ] Test YOLO detection accuracy
- [ ] Verify YouTube stream capture works
- [ ] Test heatmap generation and overlay
- [ ] Verify alert system functionality
- [ ] Test complete pipeline with Docker Compose
- [ ] Performance optimization for real-time processing

## Step 7: Documentation
- [ ] Update README.md with new features and setup instructions
"## Step 5: Create Consumer Service"  
"- [x] Create consumer/ directory with consumer.py, config.yaml, requirements.txt, Dockerfile"  
"- [x] Implement Kafka consumer to process video frames with YOLO detection"  
"- [x] Save crowd detection results to PostgreSQL database"  
"- [x] Add consumer service to docker-compose.yml"  
""  
"## Step 6: Update Dashboard for Real-time Updates"  
"- [x] Dashboard already supports real-time data from database"  
"- [x] No changes needed to dashboard/app.py"  
""  
"## Step 7: Update Airflow DAG"  
"- [x] Add consumer health check task"  
"- [x] Add database insert verification task"  
"- [x] Update task dependencies to include consumer monitoring"  
""  
"## Step 8: Testing and Verification"  
"- [ ] Test YOLO detection accuracy"  
"- [ ] Verify YouTube stream capture works"  
"- [ ] Test heatmap generation and overlay"  
"- [ ] Verify alert system functionality"  
"- [ ] Test complete pipeline with Docker Compose"  
"- [ ] Performance optimization for real-time processing"  
"- [ ] Test consumer processing and database inserts"  
""  
"## Step 9: Documentation"  
"- [ ] Update README.md with new features and setup instructions" 
