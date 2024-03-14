# Real-Time Data Processing Project with GCP Pub/Sub and DataStax Cassandra

This project demonstrates real-time data processing using Google Cloud Platform (GCP) Pub/Sub for data streaming and DataStax Cassandra for data storage. The project includes the following functionalities:

- Producing orders data in the `orders_data` topic.
- Generating payments data for orders and sending them to the `payments_data` topic.
- Consuming data from the `orders_data` topic and inserting the data into a Cassandra table. If an order ID is already present, it publishes the order record to the `dlq_orders_data` topic.
- Consuming payments data, updating payment columns in the same Cassandra table. Published to `dlq_payment_data` topic for missing orders.

## Project Structure

- `producer.py`: Python script for producing orders (orders-data-producer.py) and payments data (payments-data-producer.py) to Pub/Sub topics.
- `consumer.py`: Python script for consuming data from Pub/Sub topics and updating Cassandra tables. Orders-data-consumer.py fetches the data from order_data topic and ingest-in-fact-table.py fetches from payments_data.
- `requirements.txt`: List of Python dependencies required for the project.

## Prerequisites

Before running the project, ensure you have the following installed and set up:

- Python 3.x
- Google Cloud SDK
- DataStax Cassandra
- Access to GCP Pub/Sub and DataStax Cassandra credentials

