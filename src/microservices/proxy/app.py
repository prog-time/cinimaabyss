from flask import Flask, request, jsonify, Response
import requests
import os

app = Flask(__name__)

MOVIES_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

@app.route('/health', methods=['GET'])
def get_health():
    return {"status": "proxy-service is healthy"}, 200

@app.route('/api/movies', methods=['GET'])
def proxy_movies():
    resp = requests.get(f"{MOVIES_URL}/api/movies")
    return Response(resp.content, status=resp.status_code, content_type=resp.headers.get('Content-Type'))

@app.route('/api/events', methods=['GET'])
def proxy_monolith():
    resp = requests.get(f"{EVENTS_URL}/")
    return Response(resp.content, status=resp.status_code, content_type=resp.headers.get('Content-Type'))

@app.route('/api/users', methods=['GET'])
def get_users():
    return jsonify(["Пользователь 1", "Пользователь 2", "Пользователь 3"]), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8000)))
