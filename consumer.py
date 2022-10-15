import argparse
import json

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import pandas as pd

pd.set_option('display.max.columns', None)
API_KEY = 'N2R7SD55RFFYV7DN'
ENDPOINT_SCHEMA_URL = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'ehyW3jj4hOXm+SRZgS+69bvvrB8W8w8OOyKsfchSkMhmnx68yqJ/fIo3EVxHGSDy'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '22VAQFXMPYC4JVK2'
SCHEMA_REGISTRY_API_SECRET = 'C5XtehIc0Q1ON7SgAQUh7Egmr10wuohTfFAeffysa3pgdFPEAqt/b55540xnYjOQ'
count = 0
final_frame = pd.DataFrame()

"""
This is the third case in which we have to pull data from kafka topic and append the data into
output.csv
"""


def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Car:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_car(data: dict, ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # getting latest schema by name
    my_schema = schema_registry_client.get_latest_version("restaurent-take-away-data-value")
    # sotring the schema in my_Str
    my_str = my_schema.schema.schema_str

    json_deserializer = JSONDeserializer(my_str,
                                         from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()

    consumer_conf.update({
        'group.id': 'g',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)

    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if car is not None:
                print("User record {}: car: {}\n"
                      .format(msg.key(), car))

                global count
                count = count + 1  # variable to print no of total records

                attr = car.__getattribute__("record")

                df1 = pd.DataFrame(attr.items())
                d = df1.transpose()  # tranpose the dataframe

                if count == 1: # if this is first record than write column
                    d.to_csv('Output.csv', mode='w', index=False, header=False)

                else: # only write record not column name
                    d.drop(index=d.index[0], inplace=True) #removing the column names
                    d.to_csv('Output.csv', mode='a', index=False, header=False)

        except KeyboardInterrupt:
            break

    consumer.close()
    print("total no of record pulled from this topic " +str(count))


main("restaurent-take-away-data")