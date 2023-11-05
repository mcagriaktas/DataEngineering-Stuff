from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import pandas as pd
import time


# Create topic
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092', 'localhost:9292'])

try:
    churn = NewTopic(name='churn',num_partitions=3, replication_factor=1)
    admin_client.create_topics(new_topics=[churn])
except:
    print("churn topic already created.")

producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9292'])

# Read data
df = pd.read_csv("/home/train/datasets/Churn_Modelling.csv")
print(df.head())
df.columns


for index, row in df.iterrows():
    my_msg = f"{row['Gender']},{row['Age']},{row['EstimatedSalary']},{row['Exited']}".encode("utf-8")
    my_key = f"{row['CustomerId']}".encode("utf-8")

    producer.send('churn', key=my_key,
                   value=my_msg)
    print(index)
    time.sleep(0.1)

producer.flush()