from confluent_kafka.admin import AdminClient, NewTopic

def create_a_new_topic_if_not_exists(admin_client, topic_name, num_partitions, replication_factor):
    
    existing_topics = admin_client.list_topics().topics
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists. No action taken.")
        return
    
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )
    
    admin_client.create_topics([new_topic])
    print(f"Topic '{topic_name}' created with {num_partitions} partitions and replication factor {replication_factor}.")

if __name__ == "__main__":
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    topic_name = "medium_data"
    num_partitions = 3
    replication_factor = 2
    create_a_new_topic_if_not_exists(admin_client, topic_name, num_partitions, replication_factor)
