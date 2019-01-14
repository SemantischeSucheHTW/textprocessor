FROM python:3.7-stretch

RUN pip install kafka-python pymongo

COPY spacy_german_preprocessing /spacy_german_preprocessing
WORKDIR /spacy_german_preprocessing
RUN pip install .
RUN python -m spacy download de

RUN mkdir /textprocessor
WORKDIR /textprocessor

COPY indexdao indexdao
COPY textdao textdao
COPY urlsource urlsource
COPY wordlistsink wordlistsink

COPY main.py main.py

ENV KAFKA_BOOTSTRAP_SERVERS kafka:9092
ENV KAFKA_PAGEDETAILS_TOPIC pagedetails
ENV KAFKA_PAGEDETAILS_GROUP_ID textprocessor
ENV KAFKA_NEW_WORDLIST_TOPIC new_wordlist

ENV MONGODB_HOST mongo
ENV MONGODB_DB default
ENV MONGODB_PAGEDETAILS_COLLECTION pagedetails
ENV MONGODB_WORDINDEX_COLLECTION wordindex

ENV MONGODB_USERNAME genericparser
ENV MONGODB_PASSWORD genericparser

ENV DEBUG true

CMD ["python3", "-u", "main.py"]
