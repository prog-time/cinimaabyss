from flask import Flask, jsonify
import os

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def get_health():
    return jsonify("Микросервис movies"), 200

@app.route('/api/movies', methods=['GET'])
def get_movies():
    return jsonify(["Фильм 1", "Фильм 2", "Фильм 3"]), 200

@app.route('/api/users', methods=['GET'])
def get_users():
    return jsonify(["Пользователь 1", "Пользователь 2", "Пользователь 3"]), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8000)))