from kafka import KafkaConsumer

from urlsource.urlsource import UrlSource

class KafkaSource(UrlSource):

    def __init__(self, config):

        config["key_deserializer"] = lambda k: k.decode("utf-8")
        config["value_deserializer"] = lambda k: k.decode("utf-8")
        topic = config.pop("topic")

        self.consumer = KafkaConsumer(topic, **config)

    def getURL(self):
        kv = next(self.consumer)
        return kv.value