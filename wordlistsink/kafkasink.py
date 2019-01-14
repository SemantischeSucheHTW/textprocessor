from wordlistsink.wordlistsink import WordlistSink
from kafka import KafkaProducer

import json

class KafkaWordlistSink(WordlistSink):
    def __init__(self, config):
        config["key_serializer"] = str.encode
        config["value_serializer"] = lambda v: json.dumps(v).encode('utf-8')
        c_copy = dict(config)
        topic = c_copy.pop("topic")
        self.producer = KafkaProducer(**c_copy)
        self.topic = topic

    def send(self, url, wordlist):
        future = self.producer.send(self.topic, key=url, value=wordlist)
        future.get(timeout=10)
