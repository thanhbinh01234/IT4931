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


print("Python path:", sys.executable)

SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[8]")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
STREAM_HOST = os.environ.get("STREAM_HOST", "localhost")
STREAM_PORT = int(os.environ.get("STREAM_PORT", 5555))

TIMEZONE = pytz.timezone('Asia/Ho_Chi_Minh')

conn = MongoClient(MONGO_URL)
# connection = Connection('localhost',27017)
# Switch to the database
db = conn['twitter']
db['counts'].drop()
db['ratio'].drop()
db['tracking_word'].drop()


def write_to_file(fname, content):
    with open(fname, 'a') as f:
        f.write(content + '\n')


def tweet_count(lines):
    tweet_cnt = (
        lines.flatMap(lambda line: line.split('---'))
            .map(lambda line: 1)
            .reduce(lambda x, y: x + y)
            .foreachRDD(lambda x: write_to_file('/data/count.txt', '\n'.join(map(str, x.collect()))))
    )


def classify_tweet(tf):
    return IDF().fit(tf).transform(tf)


def sentiment_analysis(lines, hashingTF, iDF):
    model = pickle.load(open('src/model/model.ml', 'rb'))
    analysis = lines.map(lambda line: line.split()) \
        .map(lambda x: hashingTF.transform(x)) \
        .transform(classify_tweet) \
        .map(lambda x: LabeledPoint(1, x)) \
        .map(lambda x: model.predict(x.features)) \
        .reduce(lambda x, y: x + y)\
            .foreachRDD(lambda x: write_to_file('/data/ratio.txt', '\n'.join(map(str, x.collect()))))

def data_to_db(db, start_time, counts, pos, tracking_word):
    # 1) Store counts
    # Complement the counts list
    # if len(counts) < len(start_time):model = pickle.load(open('src/model/model.ml', 'rb'))
    #	counts.extend([0]*(len(start_time)-len(counts)))
    counts_t = []
    for i in range(min(len(counts), len(start_time))):
        print(str(start_time[i]))
        time = str(start_time[i]).split(" ")[1]
        time = time.split(".")[0]
        counts_t.append((time, counts[i]))
    counts_df = pd.DataFrame(counts_t, columns=['Time', 'Count'])
    counts_js = json.loads(counts_df.reset_index().to_json(orient='records'))
    db['counts'].insert(counts_js)
    # 3) Store ratio
    whole = sum(counts[:len(pos)])
    pos_whole = sum(pos)
    # Prevent divide 0:
    if whole:
        pos = 1.0 * pos_whole / whole
    else:
        pos = 1
    neg = 1 - pos
    ratio_df = pd.DataFrame([(pos, 'P'), (neg, 'N')], columns=['Ratio', 'PN'])
    ratio_js = json.loads(ratio_df.reset_index().to_json(orient='records'))
    db['ratio'].insert(ratio_js)
    # 7) Store tracking_word
    tracking_word_df = pd.DataFrame([tracking_word], columns=['Tracking_word'])
    tracking_word_js = json.loads(tracking_word_df.reset_index().to_json(orient='records'))
    db['tracking_word'].insert(tracking_word_js)


def main(sc, db, tracking_word):
    print('>' * 30 + 'SPARK START' + '>' * 30)

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

    # Store the data to MongoDB
    # data_to_db(db, start_time, tweet_cnt_li, pos_cnt_li,
    #         tracking_word)

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

    db['tracking_word'].insert({'tracking': tracking_word})

    main(sc, db, tracking_word)
