import random
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from conduktor_config import producer_config, sr_config
import pandas as pd
import time
import atexit
import os.path
from pandasql import sqldf

REFERENCE_AVSC_PATH = '/Users/anujwalia/PycharmProjects/DataEngineering/reference.avsc'

REFERENCE_DATA_PICKLE_PATH= '/Users/anujwalia/PycharmProjects/DataEngineering/data/reference_data.pickle'


def cleanup():
    x = False
    producer.flush()



atexit.register(cleanup)


def generate_reference_point(trader_id):
    trade_books = ["ONE", "TWO", "THREE", "FOUR"]
    desks = ["Desk1", "Desk2", "Desk3", "Desk4"]
    reference_data_point = {"trader_id": trader_id, "desk_id": random.choice(desks),
                            "trade_book": random.choice(trade_books),"last_update": time.time_ns()}
    return reference_data_point


schema_registry_client = SchemaRegistryClient(sr_config)
producer = Producer(producer_config)

reference_schema_string = open('%s' % REFERENCE_AVSC_PATH, 'r').read()
print(reference_schema_string)
df=pd.DataFrame()

if os.path.exists(REFERENCE_DATA_PICKLE_PATH):
    df = pd.read_pickle(REFERENCE_DATA_PICKLE_PATH)


avro_serializer = AvroSerializer(schema_registry_client, reference_schema_string)
reference_data=[]
for i in range(1, 1000):
    reference_data = []
    # producer.produce("topic","value","",on_delivery=callback())
    trader_id="user_"+str(i)
    data = generate_reference_point(trader_id)
    producer.produce(topic="reference", key=trader_id , value=avro_serializer(data,SerializationContext("reference", MessageField.VALUE)))

    reference_data.append(data)
new_df=pd.DataFrame(reference_data)
df.update(new_df)
df.to_pickle(REFERENCE_DATA_PICKLE_PATH)





# gitpod      2478       1  1 02:11 ?        00:00:12 /home/gitpod/.sdkman/candidates/java/current/bin/java
# -XX:+UseG1GC -Xmx536870902 -Xms536870902 -XX:MaxDirectMemorySize=268435458 -XX:MaxMetaspaceSize=268435456
# -Dlog.file=/workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/log/flink-gitpod-taskexecutor-2-confluentin-learnbuildi-u0c70006rhs.log
# -Dlog4j.configuration=file:/workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/conf/log4j.properties
# -Dlog4j.configurationFile=file:/workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/conf/log4j.properties
# -Dlogback.configurationFile=file:/workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/conf/logback.xml
# -classpath /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-cep-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-connector-files-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-csv-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-json-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-scala_2.12-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-table-api-java-uber-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-table-planner-loader-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-table-runtime-1.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/log4j-1.2-api-2.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/log4j-api-2.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/log4j-core-2.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/log4j-slf4j-impl-2.17.1.jar:
# /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/lib/flink-dist-1.17.1.jar:::: org.apache.flink.runtime.taskexecutor.TaskManagerRunner
# --configDir /workspace/learn-building-flink-applications-in-java-exercises/flink-1.17.1/conf
# -D taskmanager.memory.network.min=134217730b -D taskmanager.cpu.cores=1.0
# -D taskmanager.memory.task.off-heap.size=0b -D taskmanager.memory.jvm-metaspace.size=268435456b
# -D external-resources=none -D taskmanager.memory.jvm-overhead.min=201326592b
# -D taskmanager.memory.framework.off-heap.size=134217728b
# -D taskmanager.memory.network.max=134217730b -D taskmanager.memory.framework.heap.size=134217728b
# -D taskmanager.memory.managed.size=536870920b -D taskmanager.memory.task.heap.size=402653174b
# # -D taskmanager.numberOfTaskSlots=1 -D taskmanager.memory.jvm-overhead.max=201326592b
# #





