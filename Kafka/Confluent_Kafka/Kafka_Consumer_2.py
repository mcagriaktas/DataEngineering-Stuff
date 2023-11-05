from confluent_kafka import Consumer, KafkaError
import psycopg2


kafka_broker = "172.18.0.11:9092"

kafka_topic = "namepostgres3"

pg_host = "172.17.0.3"
pg_port = "5432"
pg_user = "myuser"
pg_password = "mypassword"
pg_database = "mydb"

consumer_config = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'namegroup3',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe([kafka_topic])

def insert_into_postgresql(data):
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        database=pg_database
    )
    cursor = conn.cursor()   

    lines = data.strip().split('\n')
    for line in lines:
        values = line.split(',')

        insert_query = "INSERT INTO name (id, name, point) VALUES (%s, %s, %s)"
        #cursor.execute("INSERT INTO name (id, name, point) VALUES (20, 'ali', 20)")

        cursor.execute(insert_query, values)
        conn.commit()

    
    cursor.close()
    conn.close()


if __name__ == "__main__":
    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
        else:
            data = msg.value().decode('utf-8')
            print(data)
            insert_into_postgresql(data)

