from pymongo import MongoClient

from textdao.textdao import TextDao


class MongoDBTextDao(TextDao):

    def __init__(self, config):

        conf_copy = dict(config)
        db = conf_copy.pop("db")
        pagedetails_collection_name = conf_copy.pop("pagedetails_collection")

        self.client = MongoClient(**conf_copy)
        self.db = self.client[db]
        self.pagedetails_collection = self.db[pagedetails_collection_name]

    def getText(self, url):

        doc = self.pagedetails_collection.find_one({ "_id": url })

        return doc["text"]

    def saveWords(self, url, words):

        self.pagedetails_collection.update_one(
            { "_id": url },
            { "$set": { "words": words} },
            upsert=True
        )

