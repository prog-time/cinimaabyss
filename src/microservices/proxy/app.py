from flask import Flask, request, jsonify, Response
import requests
import os
import random
import logging
from functools import wraps
from prometheus_client import Counter, Histogram, start_http_server, generate_latest

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Конфигурация сервисов
MOVIES_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
EVENTS_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"

# Метрики Prometheus
REQUEST_COUNT = Counter('request_count', 'Total request count', ['service', 'endpoint', 'status'])
RESPONSE_TIME = Histogram('response_time_seconds', 'Response time in seconds', ['service', 'endpoint'])

# Обработчик ошибок
def error_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            logger.error(f"Service request failed: {str(e)}")
            return jsonify({"error": "Service temporarily unavailable"}), 503
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500
    return wrapper

# Логика маршрутизации между микросервисом и монолитом
def should_route_to_microservice():
    if not GRADUAL_MIGRATION:
        return True
    return random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT

@app.route('/health', methods=['GET'])
def get_health():
    return {"status": "proxy-service is healthy"}, 200

@app.route('/metrics', methods=['GET'])
@error_handler
def get_metrics():
    return Response(generate_latest(), mimetype='text/plain')

@app.route('/api/movies', methods=['GET'])
@error_handler
def proxy_movies():
    service = "microservice" if should_route_to_microservice() else "monolith"
    endpoint = "/api/movies"

    try:
        with RESPONSE_TIME.labels(service=service, endpoint=endpoint).time():
            if service == "microservice":
                logger.info("Routing request to movies microservice")
                resp = requests.get(f"{MOVIES_URL}/api/movies")
            else:
                logger.info("Routing request to monolith")
                resp = requests.get(f"{MONOLITH_URL}/api/movies")

        REQUEST_COUNT.labels(service=service, endpoint=endpoint, status=resp.status_code).inc()
        return Response(resp.content, status=resp.status_code, content_type=resp.headers.get('Content-Type'))
    except Exception as e:
        REQUEST_COUNT.labels(service=service, endpoint=endpoint, status=500).inc()
        raise

@app.route('/api/events', methods=['GET'])
@error_handler
def proxy_events():
    try:
        with RESPONSE_TIME.labels(service="events", endpoint="/api/events").time():
            resp = requests.get(f"{EVENTS_URL}/")
        REQUEST_COUNT.labels(service="events", endpoint="/api/events", status=resp.status_code).inc()
        return Response(resp.content, status=resp.status_code, content_type=resp.headers.get('Content-Type'))
    except Exception as e:
        REQUEST_COUNT.labels(service="events", endpoint="/api/events", status=500).inc()
        raise

@app.route('/api/users', methods=['GET'])
@error_handler
def get_users():
    return jsonify(["Пользователь 1", "Пользователь 2", "Пользователь 3"]), 200

if __name__ == '__main__':
    # Запуск сервера метрик Prometheus на порту 9090
    start_http_server(9090)
    logger.info(f"Starting proxy service with MOVIES_MIGRATION_PERCENT={MOVIES_MIGRATION_PERCENT}%")
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8000)))
