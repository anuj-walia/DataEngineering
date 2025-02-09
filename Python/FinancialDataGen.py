#generate RFQs
from typing import Optional

#RFQs will have 3 stages, so  first you need to generate RQ in new stage put, them into a store and then
#Another thread simultaneously should pick on those RFQs and start generating other stages out of it
#In the Traded away or done stage

from pydantic import BaseModel
from datetime import datetime,timedelta
import random
import pickle

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from conduktor_config import producer_config, sr_config
import time

global_id=1
RFQ_AVSC = '/Users/anujwalia/PycharmProjects/DataEngineering/rfq.avsc'
CUSIPS_PICKLE = '/Users/anujwalia/PycharmProjects/DataEngineering/data/cusips.pickle'
RFQ_ID_LOCK = '/Users/anujwalia/PycharmProjects/DataEngineering/data/last_rfq_id.lock'
tickers=['ticker1','ticker2','ticker3','ticker4','ticker5','ticker6','ticker7','ticker8','ticker9','ticker10']
cusip_list = None
with open(CUSIPS_PICKLE, 'rb') as f:
    tickers = pickle.load(f)
class RFQ(BaseModel):
    id: str
    born_time:datetime
    # born_time:int
    receive_time:datetime
    # receive_time:int
    ticker:str
    price: float
    state:str
    completed_time:Optional[datetime] = None
    # completed_time:Optional[int] = None
    # def __init__(self, id: int, time: datetime, ticker: str, price: float, time_to_exec: datetime):
    #     self.id = id
    #     self.time = time
    #     self.ticker = ticker
    #     self.price = price
    #     self.time_to_exec = time_to_exec

def generate_rfq():
    global global_id
    global_id+=1
    # born_time=int((datetime.now() - timedelta(seconds=1)).timestamp()*1e9)
    born_time=datetime.now() - timedelta(seconds=1)
    receive_time=datetime.now()
    # receive_time=time.time_ns()
    # completed_time=datetime.now()+timedelta(seconds=1)
    # completed_time=int((datetime.now()+timedelta(seconds=1)).timestamp()*1e9)
    completed_time=datetime.now()+timedelta(seconds=5)
    ticker=random.choice(tickers)
    yield RFQ(id=str(global_id), born_time=born_time,receive_time=receive_time,state='NEW',ticker=ticker,price=0,completed_time=None)
    if random.random()>0.5:
        yield RFQ(id=str(global_id), born_time=born_time,receive_time=receive_time,state='DONE',ticker=ticker,price=random.uniform(90.0,110.0),completed_time=completed_time)
    else:
        yield RFQ(id=str(global_id), born_time=born_time, receive_time=receive_time, state='TRADED_AWAY', ticker=ticker, price=random.uniform(90.0,110.0),completed_time=completed_time)


def callback(err,event):
    if err:
        print(f'Uh Oh!  --> {err}')
    else:
        print(event)
        val = event.value().decode('utf-8', errors='ignore')
        print(f'{val} sent to partition {event.partition()}.')


# x=generate_rfq()
#
# for i in x:
#     print(i)

if __name__ == '__main__':
    x = True
    last_trans_id = 1
    schema_registry_client = SchemaRegistryClient(sr_config)
    producer = Producer(producer_config)

    rfq_schema_string = open('%s' % RFQ_AVSC, 'r').read()
    print(rfq_schema_string)

    avro_serializer = AvroSerializer(schema_registry_client, rfq_schema_string)
    while x:
        rfq = generate_rfq()
        # producer.produce("topic","value","",on_delivery=callback())

        # producer.produce(topic="rfqs", key=rfq['id'],
        #                  value=avro_serializer(rfq, SerializationContext('rfqs', MessageField.VALUE)),
        #                  on_delivery=callback)
        for x in generate_rfq():
            producer.produce(topic="rfqs", key=x.id,
                             value=avro_serializer(dict(x), SerializationContext('rfqs', MessageField.VALUE)),
                             on_delivery=callback)
        last_trans_id = last_trans_id + 1
        producer.flush(1000)
        # time.sleep(1)