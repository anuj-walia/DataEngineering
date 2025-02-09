import random
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from conduktor_config import producer_config, sr_config
import time
import pickle
import atexit
import os.path

TRANSACTION_AVSC = '/Users/anujwalia/PycharmProjects/DataEngineering/transaction.avsc'

TRANS_ID_LOCK = '/Users/anujwalia/PycharmProjects/DataEngineering/data/last_trans_id.lock'

CUSIPS_PICKLE = '/Users/anujwalia/PycharmProjects/DataEngineering/data/cusips.pickle'

"""
# Required connection configs for Kafka producer, consumer, and admin
bootstrap.servers=pkc-gq2d51.ca-west-1.aws.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=3CCAOKAEB2GTI24M
sasl.password=vhLaBjCdZ6YBYrGiTkUFHP7AimqDS3AlvoMZwf9timDzqG+1axbH6v78HXJ3IKbx

# Best practice for higher availability in librdkafka clients prior to 1.7
session.timeout.ms=45000

"""
x=True
last_trans_id = 1
def cleanup():
    x=False
    producer.flush()
    f = open(TRANS_ID_LOCK, 'w')
    f.write(str(last_trans_id-1))
    f.flush()


atexit.register(cleanup)

cusip_list = None
with open(CUSIPS_PICKLE, 'rb') as f:
    cusip_list = pickle.load(f)
last_trans_id=1 # redundant

if os.path.exists('%s' % TRANS_ID_LOCK):
    f = open(TRANS_ID_LOCK, 'r')
    last_trans_id = int(f.read().strip())
else:
    last_trans_id = 1
cusip_list

def generate_transaction():
    trade_id="tr"+str(random.randint(1000,9999))
    user_id="user_"+str(random.randint(1,999))
    amount=random.random()*1000
    instruments=["Bond","Derivative","ETF","Option","Future","Index","Treasury","Muni"]
    currencies=["USD","GBP","CAD"]
    transaction={"trade_id":trade_id, "transaction_id": last_trans_id , "cusip": random.choice(cusip_list), "user_id": user_id,
            "amount": amount, "instrument_type": random.choice(instruments),
            "currency": random.choice(currencies), "timestamp": time.time_ns()
    }

    return transaction

schema_registry_client = SchemaRegistryClient(sr_config)
producer = Producer(producer_config)

transaction_schema_string=open('%s' % TRANSACTION_AVSC, 'r').read()
print(transaction_schema_string)

avro_serializer = AvroSerializer(schema_registry_client,transaction_schema_string)


def callback(err,event):
    if err:
        print(f'Uh Oh!  --> {err}')
    else:
        print(event)
        val = event.value().decode('utf-16', errors='ignore')
        print(f'{val} sent to partition {event.partition()}.')




while x:
    transaction = generate_transaction()
    # producer.produce("topic","value","",on_delivery=callback())
    data=generate_transaction()
    producer.produce(topic="transactions",key=data['trade_id'],
                     value=avro_serializer(data, SerializationContext('transactions',MessageField.VALUE)),on_delivery=callback)
    last_trans_id=last_trans_id+1
    producer.flush(1000)
    # time.sleep(1)


