from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

import os
import time
import datetime
import pickle
import json
import pandas as pd
import sys
import pytz
from pymongo import MongoClient
import requests


print("Python path:", sys.executable)

SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[8]")
STREAM_HOST = os.environ.get("STREAM_HOST", "localhost")
STREAM_PORT = int(os.environ.get("STREAM_PORT", 5555))

SERVER_URL = os.environ.get("SERVER_URL", "localhost:5000")

TIMEZONE = pytz.timezone('Asia/Ho_Chi_Minh')

def write_to_file(fname, content):
    with open(fname, 'a') as f:
        f.write(content + '\n')


def handle_tweets(counts):
    with open('/data/count.txt', 'a') as f:
        f.write('\n'.join(map(str, counts)) + '\n')

    print("> New tweet count: ", counts)

    requests.post(
        f'{SERVER_URL}/data/counts',
        json={
            'data': [{'Time': datetime.datetime.now(tz=TIMEZONE).isoformat(), 'Count': count} for count in counts]
        }
    )


def tweet_count(lines):
    tweet_cnt = (
        lines.flatMap(lambda line: line.split('---'))
            .map(lambda line: 1)
            .reduce(lambda x, y: x + y)
            .foreachRDD(lambda x: handle_tweets(x.collect()))
    )


def classify_tweet(tf):
    return IDF().fit(tf).transform(tf)


def handle_sentiments(positives):
    with open('/data/ratio.txt', 'a') as f:
        f.write('\n'.join(map(str, positives)) + '\n')

    print("> Number of positive posts: ", positives)

    requests.post(
        f'{SERVER_URL}/data/ratio',
        json={'positive': list(positives)}
    )


def sentiment_analysis(lines, hashingTF, iDF):
    model = pickle.load(open('src/model/model.ml', 'rb'))
    analysis = lines.map(lambda line: line.split()) \
        .map(lambda x: hashingTF.transform(x)) \
        .transform(classify_tweet) \
        .map(lambda x: LabeledPoint(1, x)) \
        .map(lambda x: model.predict(x.features)) \
        .reduce(lambda x, y: x + y)\
            .foreachRDD(lambda x: handle_sentiments(x.collect()))

def main(sc, tracking_word):
    print('>' * 30 + 'SPARK START' + '>' * 30)

    requests.post(f"{SERVER_URL}/data/tracking_word", json={
        "tracking_word": tracking_word
    })

    hashingTF = HashingTF()
    iDF = IDF()

    ssc = StreamingContext(sc, batch_interval)

    # Create a DStream that represents streaming data from TCP source
    socket_stream = ssc.socketTextStream(STREAM_HOST, STREAM_PORT)
    lines = socket_stream.window(window_time)

    # Construct tables
    tmp = [('none', 0)]

    trans_table = str.maketrans('', '', ',.!?:;"@&()#.-\\/+')
    lines.map(lambda line: line.translate(trans_table).lower())

    tweet_count(lines)
    sentiment_analysis(lines, hashingTF, iDF)

    ###########################################################################
    # Start the streaming process
    ssc.start()

    process_cnt = 0
    start_time = [datetime.datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh'))]
    # print("Here!!!", process_times, process_cnt)

    while process_cnt < process_times:
        time.sleep(window_time)
        start_time.append(datetime.datetime.now(tz=pytz.timezone('Asia/Ho_Chi_Minh')))

    ssc.stop()
    ###########################################################################

    print('>' * 30 + 'SPARK STOP' + '>' * 30)


if __name__ == "__main__":
    # Define Spark configuration
    conf = SparkConf().setMaster(SPARK_MASTER).setAppName("Twitter-Hashtag-Tracking")
    # Initialize a SparkContext
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # Initialize sparksql context
    # Will be used to query the trends from the result.
    sqlContext = SQLContext(sc)
    # Initialize the tweet_cnt_li

    # Load parameters
    with open('conf/parameters.json') as f:
        p = json.load(f)
        tracking_word = p['keyword']
        batch_interval = int(p['DStream']['batch_interval'])
        window_time = int(p['DStream']['window_time'])
        process_times = int(p['DStream']['process_times'])

    main(sc, tracking_word)
