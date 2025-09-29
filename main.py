import sys
import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify
import mysql.connector

# ==============================
# Logger Setup
# ==============================
def setup_logging():
    logs_dir = './logs'
    os.makedirs(logs_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

    fh = logging.FileHandler(os.path.join(logs_dir, 'app.log'), encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    logger.info("="*40)
    logger.info("Alphabet Alchemist API Starting")
    logger.info("="*40)
    return logger

logger = setup_logging()

# ==============================
# Converter Logic
# ==============================
class MeasurementConverter:
    def __init__(self):
        pass

    def _val(self, ch):
        if ch == "_":
            return 0
        return ord(ch) - ord('a') + 1

    def _parse_element(self, s, i):
        """Parse one element considering the z-rule."""
        if i >= len(s):
            return 0, i
        if s[i] != 'z':
            return self._val(s[i]), i + 1
        
        # Handle z-groups
        total = 0
        while i < len(s) and s[i] == 'z':
            total += 26
            i += 1
        if i < len(s):
            total += self._val(s[i])
            i += 1
        return total, i

    def convert(self, s):
        s = s.strip().lower()
        i = 0
        n = len(s)
        result = []

        while i < n:
            # Parse count
            if s[i] == "_":
                result.append(0)
                i += 1
                continue

            count, i = self._parse_element(s, i)

            if count == 0:
                result.append(0)
                continue

            # Parse values based on count
            package_sum = 0
            read = 0
            while read < count and i < n:
                val, i = self._parse_element(s, i)
                package_sum += val
                read += 1

            # If last element is "_" as VALUE, include it in sum but not as separate package
            result.append(package_sum)

        return result

# ==============================
# Database Manager
# ==============================
class DatabaseManager:
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'mysql'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'rootpassword'),
            'database': os.getenv('DB_NAME', 'measurement_db'),
            'port': int(os.getenv('DB_PORT', 3306))
        }

    def get_connection(self):
        return mysql.connector.connect(**self.config)

    def init_database(self):
        try:
            temp = self.config.copy()
            dbname = temp.pop("database")
            conn = mysql.connector.connect(**temp)
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
            conn.commit()
            cur.close()
            conn.close()

            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversion_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    input_string VARCHAR(2000),
                    output_result TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"DB init error: {e}")

    def store_conversion(self, input_string, result):
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO conversion_history (input_string, output_result) VALUES (%s, %s)",
                (input_string, json.dumps(result))
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"DB store error: {e}")

    def get_history(self, limit=100):
        try:
            conn = self.get_connection()
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT * FROM conversion_history ORDER BY id DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [
                {
                    "id": r["id"],
                    "input": r["input_string"],
                    "output": json.loads(r["output_result"]),
                    "timestamp": r["created_at"].isoformat() if r["created_at"] else None
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"DB history error: {e}")
            return []

# ==============================
# Flask API
# ==============================
app = Flask(__name__)
converter = MeasurementConverter()
db_manager = DatabaseManager()

@app.route("/convert-measurements", methods=["GET"])
def convert_measurements():
    input_string = request.args.get("input", "")
    if not input_string:
        return jsonify({"error": "Input parameter is required"}), 400
    try:
        result = converter.convert(input_string)
        db_manager.store_conversion(input_string, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Conversion error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/history", methods=["GET"])
def get_history():
    history = db_manager.get_history()
    return jsonify({"total_records": len(history), "history": history})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

# ==============================
# Main
# ==============================
def main():
    port = 8080
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except:
            pass
    db_manager.init_database()
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()
