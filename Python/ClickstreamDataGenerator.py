# from kafka.producer import KafkaProducer
import json
import time
import random
import avro.schema as avroschema
import avro.io as avroio
from avro.datafile import DataFileReader, DataFileWriter


def generate_clickstream_data() -> dict[str,int]:
    users = ["user1", "user2", "user3", "user4"]
    pages = ["home", "product", "search", "cart", "checkout"]
    return {
        "user": random.choice(users),
        "page": random.choice(pages),
        "timestamp": int(time.time() * 1000)
    }

clickevent_schema=avroschema.parse(
    open('/clickevent.avsc', 'r').read(),
    True,
    True
)


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: avroio.DatumWriter.write(v)
    # lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = generate_clickstream_data()
    data_array = [data]
    print(type(data_array))
    data_array.append(generate_clickstream_data())

    clickevent_schema = avroschema.parse(open("/clickevent.avsc", "rb").read())

    writer = DataFileWriter(open("/clieckevents.avro", "wb"),
                            avroio.DatumWriter(), clickevent_schema)
    writer.append(data)
    # writer.append(data_array) array cannot be written as avro library
    #   does not extrapolate elements and will show schema mismatch
    writer.close()
    producer.send('clickstream', value=data)
    time.sleep(1)
    exit(0)
