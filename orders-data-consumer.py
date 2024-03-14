import json
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Initialize the Pub/Sub subscriber and publisher clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# Project and Topic details
project_id = "your-gcp-project-id"  # Replace with your actual GCP project ID
subscription_name = "subscription-name" 
subscription_path = subscriber.subscription_path(project_id, subscription_name)
dlq_topic_path = publisher.topic_path(project_id, "dlq_orders_data")

# Cassandra config
cloud_config = {
    'secure_connect_bundle': 'path/to/secure-connect-orders-db.zip'  # Replace with the correct path
}

# Load Google Cloud credentials
with open("path/to/service-account-key.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["clientSecret"]

# Authenticate with Cassandra
auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

try:
    query = "use ecom_store"
    session.execute(query)
    print("Inside the ecom_store keyspace")
    
except Exception as err:
    print("Exception:", err)

# Prepare the Cassandra insertion statement
insert_stmt = session.prepare("""
    INSERT INTO orders_payments_facts (order_id, customer_id, item, quantity, price, shipping_address, order_status, creation_date, payment_id, payment_method, card_last_four, payment_status, payment_datetime)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Pull and process messages
def pull_messages():
    while True:
        response = subscriber.pull(request={"subscription": subscription_path, "max_messages": 10})
        ack_ids = []

        for received_message in response.received_messages:
            # Extract JSON data
            json_data = received_message.message.data.decode('utf-8')
            
            # Deserialize the JSON data
            deserialized_data = json.loads(json_data)

            # Check if order_id exists in the Cassandra table
            query = f"SELECT order_id FROM orders_payments_facts WHERE order_id = {deserialized_data.get('order_id')}"
            rows = session.execute(query)
            if rows.one():  # if order_id found
                publisher.publish(dlq_topic_path, data=json_data.encode('utf-8'))
                print("Data thrown in DLQ because order_id is already present ->", deserialized_data)

            else:
                # Prepare data for Cassandra insertion
                cassandra_data = (
                    deserialized_data.get("order_id"),
                    deserialized_data.get("customer_id"),
                    deserialized_data.get("item"),
                    deserialized_data.get("quantity"),
                    deserialized_data.get("price"),
                    deserialized_data.get("shipping_address"),
                    deserialized_data.get("order_status"),
                    deserialized_data.get("creation_date"),
                    None,
                    None,
                    None,
                    None,
                    None
                )
                # Insert data into Cassandra
                session.execute(insert_stmt, cassandra_data)

                print("Data inserted in Cassandra ->", deserialized_data)
                
            # Collect ack ID for acknowledgment
            ack_ids.append(received_message.ack_id)

        # Acknowledge the messages so they won't be sent again
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})

# Run the consumer
if __name__ == "__main__":
    try:
        pull_messages()
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up any resources
        cluster.shutdown()
