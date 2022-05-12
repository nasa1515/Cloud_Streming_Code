from confluent_kafka import Producer
import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <topic>\n' % sys.argv[0])
        sys.exit(1)
    topic = sys.argv[1]
    conf = {
        'bootstrap.servers': 'nasa1515-eventhub01.servicebus.windows.net:9093', #replace
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': 'Endpoint=sb://nasa1515-eventhub01.servicebus.windows.net/;SharedAccessKeyName=nasa1515;SharedAccessKey=Zsx0EVRcE9+M/uCjaSbXFfQeQpjcD/otGgPIrZHqGN8=',          #replace
        'client.id': 'nasa1515-producer'
    }
    # Create Producer instance
    p = Producer(**conf)


    # fail check def
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))

    # Write 1-100 to topic
    for i in range(0, 1000):
        try:
            p.produce(topic, 'Kafka_data_nasa1515-' + str(i), callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()