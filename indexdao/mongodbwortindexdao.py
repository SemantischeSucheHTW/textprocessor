from pymongo import MongoClient

from indexdao import IndexDao


class MongoDBWortIndexDao(IndexDao):

    def __init__(self, config):

        c_copy = dict(config)
        db = c_copy.pop("db")
        wordindex_collection = c_copy.pop("wordindex_collection")

        self.client = MongoClient(**c_copy)
        self.db = self.client[db]
        self.wortindex_collection = self.db[wordindex_collection]

    def updateIndex(self, updateValue: (str, str, int)):

        (word, url, count) = updateValue
        self.wortindex_collection.update_one(
            { "word": word },
            { '$addToSet': {
                "urls_counts" : {
                    "url" : url,
                    "count" : count
                }
            }},
            upsert=True
        )

    def getUrlsAndCountsfromKey(self, searchKey):
        assert isinstance(searchKey, str)
        docs = self.wortindex_collection.find({ "word": searchKey.lower() })
        urls_counts = []
        for doc in docs:
            for url_count in doc["urls_counts"]:
                urls_counts.append( (url_count["url"], url_count["count"]) )

        return urls_counts
