# Dashboard
import os
from typing import Iterable
from flask import Flask, request, jsonify
from flask import render_template
from pymongo import MongoClient
import json
from bson import json_util

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
DBS_NAME = 'twitter'

COLLECTION_NAME2 = 'counts'
COLLECTION_NAME3 = 'ratio'
COLLECTION_NAME4 = 'tracking_word'
COLLECTION_NAME5 = 'users'
COLLECTION_NAME6 = 'time'

FIELDS2 = {'_id': False}
FIELDS3 = {'_id': False}
FIELDS4 = {'_id': False}
FIELDS5 = {'_id': False}
FIELDS6 = {'_id': False}

tweet_counts = []  # Count, Time
positives = []
curr_tracking_word = ''

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/data/counts", methods=['GET', 'POST'])
def counts():
    global tweet_counts
    if request.method == 'GET':
        return json.dumps(tweet_counts)
    if request.method == 'POST':
        tweet_counts.extend(request.get_json(force=True).get('data', []))
        return "", 200

@app.route("/data/ratio", methods=['GET', 'POST'])
def ratio():
    global positives

    if request.method == 'GET':
        total_tweets = sum(x['Count'] for x in tweet_counts[:len(positives)])
        total_pos = sum(positives)

        return jsonify([
            {
                'PN': 'P',
                'Ratio': 1 if total_tweets == 0 else total_pos / total_tweets
            },
            {
                'PN': 'N',
                'Ratio': 0 if total_tweets == 0 else (total_tweets - total_pos) / total_tweets
            }
        ])

    if request.method == 'POST':
        positives.extend(request.get_json(force=True).get('positive', []))
        return "", 200

@app.route("/data/tracking_word", methods=["GET", 'POST'])
def tracking_word():
    global curr_tracking_word

    if request.method == 'GET':
        return jsonify(tracking_word=curr_tracking_word)
    if request.method == 'POST':
        curr_tracking_word = request.get_json(force=True).get('tracking_word', '')
        return "", 200


if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)
