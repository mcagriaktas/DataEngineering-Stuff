from confluent_kafka import Producer, Consumer, KafkaError
from config import config


def produce_geographical_regions():
    producer = Producer(config)

    regions = {
        1: 'Marmara',
        2: 'Ege',
        3: 'Akdeniz',
        4: 'Ic Anadolu',
        5: "Dogu Anadolu",
        6: "Guney Doguanadolu",
        7: "Karadeniz"
    }

    for key, value in regions.items():
        producer.produce(
            topic="turkey_geographical_regions",
            key=str(key).encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_report
        )
        
    producer.flush()

def delivery_report(err, msg):
    if err is not None:
        print(f'Error: {err}')
    else:
        key = msg.key().decode('utf-8')  
        value = msg.value().decode('utf-8')  
        print(f'Key: {key}, Value: {value}, Partition: {msg.partition()}, TS: {msg.timestamp()}')

def consume_geographical_regions():
    consumer = Consumer({
        'bootstrap.servers': '172.18.0.11:9092,172.18.0.12:9092,172.18.0.13:9092', 
        'group.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['turkey_geographical_regions'])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f'Consumer error: {msg.error()}')
                break

        
        print(f'Key: {msg.key()}, Value: {msg.value()}, Partition: {msg.partition()}, TS: {msg.timestamp()}')

if __name__ == '__main__':
    produce_geographical_regions()

