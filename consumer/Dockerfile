FROM python:3.9

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install CPU-only PyTorch wheels to avoid pulling CUDA dependencies
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu \
    torch==2.3.1 \
    torchvision==0.18.1 \
    torchaudio==2.3.1

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download YOLOv8 model
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"

# Copy application code
COPY consumer.py .
COPY config.yaml .

CMD ["python", "consumer.py"]
