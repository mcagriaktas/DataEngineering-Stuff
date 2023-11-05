import csv
import requests
from kafka import KafkaProducer, KafkaAdminClient, NewTopic

bootstrap_servers = "172.18.0.11:9092,172.18.0.12:9292,172.18.0.13:9392"

topic_name = "churn"

admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

new_topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
admin_client.create_topics([new_topic])

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    key_serializer=str.encode,
    value_serializer=str.encode,
)

csv_url = "https://raw.githubusercontent.com/erkansirin78/datasets/master/Churn_Modelling.csv"
response = requests.get(csv_url)
csv_data = response.text.splitlines()
csv_reader = csv.DictReader(csv_data)

for row in csv_reader:
    producer.send(topic_name, key=row["CustomerId"], value=str(row))

producer.close()
