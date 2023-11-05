from flask import Flask, render_template, request
import redis

app = Flask(__name__)
redis_client = redis.Redis(host='redis', port=6379)  # Assuming Redis is running as a service named 'redis' in your Docker Compose setup

@app.route('/')
def start():
    history_bytes = redis_client.lrange('calculations', 0, -1)  # Retrieve the entire calculation history from Redis as bytes
    history = [calculation.decode('utf-8') for calculation in history_bytes]  # Decode bytes to string
    return render_template('index.html', history=history)

@app.route('/calculate', methods=['POST'])
def calculate():
    a = int(request.form['a'])
    b = int(request.form['b'])
    c = a + b

    calculation = f"{a} + {b} = {c}"
    redis_client.lpush('calculations', calculation)  # Store the new calculation in Redis

    history_bytes = redis_client.lrange('calculations', 0, -1)  # Retrieve the updated calculation history from Redis as bytes
    history = [calculation.decode('utf-8') for calculation in history_bytes]  # Decode bytes to string
    return render_template('index.html', history=history)

if __name__ == '__main__':
    app.run(debug=True)
