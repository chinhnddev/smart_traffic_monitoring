from flask import Flask, jsonify, render_template, request
import psycopg2
import psycopg2.extras
import os
import logging
from datetime import datetime, timedelta

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Cấu hình database
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'crowd_db'),
    'user': os.getenv('POSTGRES_USER', 'crowd_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'crowd_pass'),
    'port': 5432
}

def get_db_connection():
    """Tạo kết nối database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Lỗi kết nối database: {e}")
        return None

@app.route('/')
def index():
    """Trang chủ dashboard"""
    return render_template('index.html')

@app.route('/api/statistics')
def get_statistics():
    """API lấy thống kê theo location_id và time range"""
    try:
        location_id = request.args.get('location_id', 'all')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # Mặc định lấy dữ liệu 7 ngày gần nhất nếu không có filter
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Không thể kết nối database'}), 500

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query thống kê
        if location_id == 'all':
            query = """
                SELECT
                    location_id,
                    COUNT(*) as total_frames,
                    AVG(crowd_count) as avg_crowd,
                    MAX(crowd_count) as peak_crowd,
                    MIN(crowd_count) as min_crowd,
                    DATE(timestamp) as date
                FROM crowd_results
                WHERE DATE(timestamp) BETWEEN %s AND %s
                GROUP BY location_id, DATE(timestamp)
                ORDER BY date DESC, location_id
            """
            cursor.execute(query, (start_date, end_date))
        else:
            query = """
                SELECT
                    location_id,
                    COUNT(*) as total_frames,
                    AVG(crowd_count) as avg_crowd,
                    MAX(crowd_count) as peak_crowd,
                    MIN(crowd_count) as min_crowd,
                    DATE(timestamp) as date
                FROM crowd_results
                WHERE location_id = %s AND DATE(timestamp) BETWEEN %s AND %s
                GROUP BY location_id, DATE(timestamp)
                ORDER BY date DESC
            """
            cursor.execute(query, (location_id, start_date, end_date))

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(results)

    except Exception as e:
        logger.error(f"Lỗi API statistics: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/latest')
def get_latest():
    """API lấy 50 records mới nhất"""
    try:
        limit = int(request.args.get('limit', 50))

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Không thể kết nối database'}), 500

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT * FROM crowd_results
            ORDER BY timestamp DESC
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(results)

    except Exception as e:
        logger.error(f"Lỗi API latest: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart-data')
def get_chart_data():
    """API lấy dữ liệu cho charts theo giờ/ngày"""
    try:
        location_id = request.args.get('location_id', 'all')
        time_range = request.args.get('time_range', 'hour')  # hour hoặc day

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Không thể kết nối database'}), 500

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if time_range == 'hour':
            # Group by hour
            if location_id == 'all':
                query = """
                    SELECT
                        DATE_TRUNC('hour', timestamp) as time_period,
                        location_id,
                        AVG(crowd_count) as avg_crowd,
                        MAX(crowd_count) as peak_crowd
                    FROM crowd_results
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY DATE_TRUNC('hour', timestamp), location_id
                    ORDER BY time_period DESC, location_id
                """
                cursor.execute(query)
            else:
                query = """
                    SELECT
                        DATE_TRUNC('hour', timestamp) as time_period,
                        location_id,
                        AVG(crowd_count) as avg_crowd,
                        MAX(crowd_count) as peak_crowd
                    FROM crowd_results
                    WHERE location_id = %s AND timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY DATE_TRUNC('hour', timestamp), location_id
                    ORDER BY time_period DESC
                """
                cursor.execute(query, (location_id,))
        else:
            # Group by day
            if location_id == 'all':
                query = """
                    SELECT
                        DATE(timestamp) as time_period,
                        location_id,
                        AVG(crowd_count) as avg_crowd,
                        MAX(crowd_count) as peak_crowd
                    FROM crowd_results
                    WHERE timestamp >= NOW() - INTERVAL '7 days'
                    GROUP BY DATE(timestamp), location_id
                    ORDER BY time_period DESC, location_id
                """
                cursor.execute(query)
            else:
                query = """
                    SELECT
                        DATE(timestamp) as time_period,
                        location_id,
                        AVG(crowd_count) as avg_crowd,
                        MAX(crowd_count) as peak_crowd
                    FROM crowd_results
                    WHERE location_id = %s AND timestamp >= NOW() - INTERVAL '7 days'
                    GROUP BY DATE(timestamp), location_id
                    ORDER BY time_period DESC
                """
                cursor.execute(query, (location_id,))

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(results)

    except Exception as e:
        logger.error(f"Lỗi API chart-data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary')
def get_summary():
    """API lấy summary tổng quan"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Không thể kết nối database'}), 500

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Tổng quan
        query = """
            SELECT
                COUNT(*) as total_frames,
                COUNT(DISTINCT location_id) as total_locations,
                AVG(crowd_count) as overall_avg_crowd,
                MAX(crowd_count) as overall_peak_crowd,
                MIN(crowd_count) as overall_min_crowd
            FROM crowd_results
        """
        cursor.execute(query)
        summary = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify(dict(summary))

    except Exception as e:
        logger.error(f"Lỗi API summary: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
