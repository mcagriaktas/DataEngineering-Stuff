from confluent_kafka import Consumer, KafkaError
import os


consumer_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'flower_consumer_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['topic1'])

output_dir = '/tmp/kafka_out/'

os.makedirs(output_dir, exist_ok=True)

flower_species = ['setosa', 'versicolor', 'virginica']
output_files = {species: open(os.path.join(output_dir, f'{species}_out.txt'), 'w') for species in flower_species}
other_out_file = open(os.path.join(output_dir, 'other_out.txt'), 'w')

try:
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

        message_content = msg.value().decode('utf-8')

        for species in flower_species:
            if species in message_content:
                output_files[species].write(f'Topic: {msg.topic()}, Partition: {msg.partition()}, Message: {message_content}\n')
                break
        else:
            other_out_file.write(f'Topic: {msg.topic()}, Partition: {msg.partition()}, Message: {message_content}\n')

except KeyboardInterrupt:
    pass
finally:
    for file in output_files.values():
        file.close()
    other_out_file.close()
    consumer.close()
