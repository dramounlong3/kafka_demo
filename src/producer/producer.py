from confluent_kafka import Producer
import socket

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced: {msg.value()}")

conf = {
    'bootstrap.servers': 'localhost:9091',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

for i in range(2):
    producer.produce('my_topic', key=f'key{i}', value=f'value{i}', callback=acked)

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.flush(1)
