import random
import pickle
import uuid

from pydantic import BaseModel
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from conduktor_config import producer_config, sr_config
import time

CUSIPS_PICKLE = '/Users/anujwalia/PycharmProjects/DataEngineering/data/cusips.pickle'
STACK_AVSC = '/Users/anujwalia/PycharmProjects/DataEngineering/stack.avsc'
_venue_list='BLOOMBERG','LSEG','Tradeweb','MarketAxes','IEX'
cusip_list = None
with open(CUSIPS_PICKLE, 'rb') as f:
    cusip_list = pickle.load(f)

class Stack(BaseModel):
    id: str
    cusip:str
    venue:str
    bid:float
    ask: float
    # timestamp:datetime
    timestamp:datetime

def generate_stack():
    return Stack(id=str(uuid.uuid4()),cusip=random.choice(cusip_list),venue=random.choice(_venue_list),
                 bid=random.uniform(99,101),ask=random.uniform(99,101),timestamp=datetime.now())
                 # bid=random.uniform(99,101),ask=random.uniform(99,101),timestamp=time.time_ns())

def callback(err,event):
    if err:
        print(f'Uh Oh!  --> {err}')
    else:
        print(event)
        val = event.value().decode('utf-8', errors='ignore')
        print(f'{val} sent to partition {event.partition()}.')

if __name__ == '__main__':
    x = True
    schema_registry_client = SchemaRegistryClient(sr_config)
    producer = Producer(producer_config)

    stack_schema_string = open('%s' % STACK_AVSC, 'r').read()
    print(stack_schema_string)

    avro_serializer = AvroSerializer(schema_registry_client, stack_schema_string)
    while x:
        stack=generate_stack()
        producer.produce(topic="stacks", key=stack.id,
                             value=avro_serializer(dict(stack), SerializationContext('stacks', MessageField.VALUE)),
                             on_delivery=callback)
        producer.flush(1000)
        # time.sleep(1)