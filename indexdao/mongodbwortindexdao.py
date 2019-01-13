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
            {
                "$push": {
                    "urls_counts": { "url": url, "count": count }
                }
            },
            upsert=True
        )

    def getUrlsAndCountsfromKey(self, searchKey):
        docs = self.wortindex_collection.find({ "word": searchKey })
        for doc in docs:
            try:
                assert "url" in doc
                assert isinstance(doc["url"], str)
                assert "count" in doc
                assert isinstance(doc["count", int])
                yield (doc["url"], doc["count"])
            except AssertionError:
                print(f"Failed to validate document {doc}. Skipping!")