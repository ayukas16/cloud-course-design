import os
from flask import Flask, jsonify
import redis
import requests

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

def get_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD if REDIS_PASSWORD else None,
        decode_responses=True
    )

@app.route("/api/ping")
def ping():
    r = get_redis()
    count = r.incr("ping_count")
    return jsonify({
        "status": "ok",
        "message": "backend connected redis",
        "count": count
    })

@app.route("/")
def index():
    return jsonify({"service": "cloud-course-backend"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)