import os

from indexdao import MongoDBWortIndexDao
from spacy_preprocessing.preprocess import Preprocess
from textdao import MongoDBTextDao
from urlsource import KafkaSource
from wordlistsink import KafkaWordlistSink


def env(key):
    value = os.environ.get(key)
    if not value:
        raise Exception(f"environment variable {key} not set!")
    return value


debug = env("DEBUG")

urlSource = KafkaSource({
    "topic": env("KAFKA_PAGEDETAILS_TOPIC"),
    "bootstrap_servers": env("KAFKA_BOOTSTRAP_SERVERS"),
    "group_id": env("KAFKA_PAGEDETAILS_GROUP_ID"),
    "auto_offset_reset": "earliest"
})

textdao = MongoDBTextDao({
    "host": env("MONGODB_HOST"),
    "db": env("MONGODB_DB"),
    "pagedetails_collection": env("MONGODB_PAGEDETAILS_COLLECTION"),
    "username": env("MONGODB_USERNAME"),
    "password": env("MONGODB_PASSWORD"),
    "authSource": env("MONGODB_DB")
})

wortindexdao = MongoDBWortIndexDao({
    "host": env("MONGODB_HOST"),
    "db": env("MONGODB_DB"),
    "wordindex_collection": env("MONGODB_WORDINDEX_COLLECTION"),
    "username": env("MONGODB_USERNAME"),
    "password": env("MONGODB_PASSWORD"),
    "authSource": env("MONGODB_DB")
})

wordlistsink = KafkaWordlistSink({
    "topic": env("KAFKA_NEW_WORDLIST_TOPIC"),
    "bootstrap_servers": env("KAFKA_BOOTSTRAP_SERVERS"),
})

while True:
    url = urlSource.getURL()

    if debug:
        print(f"Got url {url}.")

    (title, text) = textdao.getTitleAndText(url)

    if len(text) > 0:
        lemma_words = Preprocess(text).preprocess(sentence_split=False, with_pos=False, do_lemma=True)

        lemma_words.extend(Preprocess(title).preprocess(sentence_split=False, with_pos=False, do_lemma=True))

        wordcounts = {}
        for word in lemma_words:
            if word in wordcounts:
                wordcounts[word] = wordcounts[word] + 1
            else:
                wordcounts[word] = 1

        for word, count in wordcounts.items():
            wortindexdao.updateIndex((word, url, count))

        if debug:
            print(f"Updated indices for {len(wordcounts)} lemma_words.")

        non_lemma_words = Preprocess(text).preprocess(sentence_split=False, with_pos=False, do_lemma=False)
        non_lemma_words.append(Preprocess(title).preprocess(sentence_split=False, with_pos=False, do_lemma=False))
        non_lemma_words = [word.lower() for word in non_lemma_words]

        textdao.saveWords(url, non_lemma_words, lemma_words)

        wordlistsink.send(url, non_lemma_words)
